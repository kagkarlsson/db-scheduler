package com.github.kagkarlsson.scheduler.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.helper.TestableListener;
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

    OneTimeTask<Void> onetime = Tasks.oneTime("onetime").execute(TestTasks.DO_NOTHING);

    TestableListener testableListener = new TestableListener(false, Collections.emptyList());
    // Missing task with name 'onetime'
    ManualScheduler scheduler =
        TestHelper.createManualScheduler(postgres.getDataSource())
            .clock(clock)
            .addSchedulerListener(testableListener)
            .build();

    scheduler.schedule(onetime.instance("id1"), clock.now());
    assertEquals(0, testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK));

    scheduler.runAnyDueExecutions();
    assertEquals(1, testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK));

    assertEquals(1, DbUtils.countExecutions(postgres.getDataSource()));

    scheduler.runDeadExecutionDetection();
    assertEquals(1, DbUtils.countExecutions(postgres.getDataSource()));

    clock.set(clock.now().plus(Duration.ofDays(30)));
    scheduler.runDeadExecutionDetection();
    assertEquals(0, DbUtils.countExecutions(postgres.getDataSource()));

    scheduler.runDeadExecutionDetection();
  }
}
