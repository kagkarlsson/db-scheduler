package com.github.kagkarlsson.scheduler.feature;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class OnCompleteKeepTest {

  private final SettableClock clock = new SettableClock();

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @Test
  public void should_keep_execution_with_state_complete_after_successful_run() {
    var scheduler =
        createManualScheduler(
            Tasks.oneTime(TestTasks.ONETIME)
                .onCompleteKeep(State.COMPLETE)
                .execute(TestTasks.DO_NOTHING));

    scheduler.schedule(TestTasks.ONETIME.instance("1").scheduledTo(clock.now()));
    scheduler.runAnyDueExecutions();

    assertThat(scheduler.getScheduledExecutions()).isEmpty();

    assertThat(getDeactivatedExecutions(scheduler))
        .singleElement()
        .satisfies(it -> assertThat(it.getState()).isEqualTo(State.COMPLETE));

    // No cleanup directly after run
    scheduler.runDeleteOldDeactivatedExecutions();
    assertThat(getDeactivatedExecutions(scheduler)).hasSize(1);

    // Cleanup after 14d
    clock.tick(Duration.ofDays(15));
    scheduler.runDeleteOldDeactivatedExecutions();
    assertThat(getDeactivatedExecutions(scheduler)).hasSize(0);
  }

  private ManualScheduler createManualScheduler(OneTimeTask<Void> task) {
    return TestHelper.createManualScheduler(postgres.getDataSource(), task).clock(clock).build();
  }

  private List<ScheduledExecution<Object>> getDeactivatedExecutions(ManualScheduler scheduler) {
    List<ScheduledExecution<Object>> executions = new ArrayList<>();
    scheduler.fetchScheduledExecutions(ScheduledExecutionsFilter.deactivated(), executions::add);
    return executions;
  }
}
