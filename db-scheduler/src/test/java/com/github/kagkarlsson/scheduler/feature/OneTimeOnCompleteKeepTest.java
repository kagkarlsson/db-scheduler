package com.github.kagkarlsson.scheduler.feature;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.DeactivatedExecution;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class OneTimeOnCompleteKeepTest {

  private SettableClock clock;

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @BeforeEach
  public void setUp() {
    clock = new SettableClock();
    clock.set(ZonedDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneId.systemDefault()).toInstant());
  }

  @Test
  public void should_keep_execution_with_state_complete() {
    OneTimeTask<Void> task =
        Tasks.oneTime(TestTasks.ONETIME)
            .onCompleteKeep(State.COMPLETE)
            .execute(TestTasks.DO_NOTHING);

    ManualScheduler scheduler =
        TestHelper.createManualScheduler(postgres.getDataSource(), task)
            .clock(clock)
            .build();

    scheduler.schedule(task.instance("1"), clock.now());
    scheduler.runAnyDueExecutions();

    assertThat(scheduler.getScheduledExecutions()).isEmpty();

    List<DeactivatedExecution> deactivated = scheduler.getDeactivatedExecutions();
    assertThat(deactivated).hasSize(1);

    DeactivatedExecution kept = deactivated.get(0);
    assertThat(kept.state()).isEqualTo(State.COMPLETE);
    assertThat(kept.lastSuccess()).isNotNull();
    assertThat(kept.lastFailure()).isNull();
  }
}
