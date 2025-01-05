package com.github.kagkarlsson.scheduler.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class DeleteUnresolvedTest {

  public static final ZoneId ZONE = ZoneId.systemDefault();
  private static final LocalDate DATE = LocalDate.of(2018, 3, 1);
  private static final LocalTime TIME = LocalTime.of(8, 0);
  private SettableClock clock;

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @BeforeEach
  public void setUp() {
    clock = new SettableClock();
    clock.set(ZonedDateTime.of(DATE, TIME, ZONE).toInstant());
  }

  @Test
  public void should_delete_executions_with_old_unresolved_tasknames() {

    OneTimeTask<Void> first = Tasks.oneTime("onetime_first").execute(TestTasks.DO_NOTHING);
    OneTimeTask<Void> second = Tasks.oneTime("onetime_second").execute(TestTasks.DO_NOTHING);

    TestableRegistry testableRegistry = new TestableRegistry(false, Collections.emptyList());
    // Missing tasks with name 'onetime_first' and 'onetime_second'
    ManualScheduler scheduler =
        TestHelper.createManualScheduler(postgres.getDataSource())
            .clock(clock)
            .statsRegistry(testableRegistry)
            .deleteUnresolvedAfter(Duration.ofDays(5))
            .build();

    scheduler.schedule(first.instance("id_f"), clock.now());
    scheduler.schedule(second.instance("id_s"), clock.now().minus(1, ChronoUnit.DAYS));
    scheduler.schedule(second.instance("id_s_2"), clock.now().minus(2, ChronoUnit.DAYS));
    assertEquals(0, testableRegistry.getCount(StatsRegistry.SchedulerStatsEvent.UNRESOLVED_TASK));

    scheduler.runAnyDueExecutions();
    assertEquals(3, testableRegistry.getCount(StatsRegistry.SchedulerStatsEvent.UNRESOLVED_TASK));

    assertEquals(3, DbUtils.countExecutions(postgres.getDataSource()));

    scheduler.runDeadExecutionDetection();
    assertEquals(3, DbUtils.countExecutions(postgres.getDataSource()));

    clock.tick(Duration.ofDays(4));
    scheduler.runDeadExecutionDetection();
    assertEquals(1, DbUtils.countExecutions(postgres.getDataSource()));

    clock.tick(Duration.ofDays(1));
    scheduler.runDeadExecutionDetection();
    assertEquals(0, DbUtils.countExecutions(postgres.getDataSource()));

    scheduler.runDeadExecutionDetection();
  }
}
