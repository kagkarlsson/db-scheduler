package com.github.kagkarlsson.scheduler.feature;

import static com.github.kagkarlsson.scheduler.TestTasks.ONETIME;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.SchedulerTester;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class OnCompleteKeepTest {

  private final SettableClock clock = new SettableClock();
  private final SchedulableInstance<Void> INSTANCE = ONETIME.instance("1").scheduledTo(clock.now());

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @Test
  public void should_keep_execution_with_state_complete_after_successful_run() {
    var scheduler =
        createManualScheduler(
            Tasks.oneTime(ONETIME).onCompleteKeep(State.COMPLETE).execute(TestTasks.DO_NOTHING));
    var tester = new SchedulerTester(scheduler);

    scheduler.schedule(INSTANCE);
    scheduler.runAnyDueExecutions();

    tester.assertThatExecution(INSTANCE).hasState(State.COMPLETE);

    // No cleanup directly after run
    scheduler.runDeleteOldDeactivatedExecutions();
    tester.assertThatExecution(INSTANCE).hasState(State.COMPLETE);

    // Cleanup after 14d
    clock.tick(Duration.ofDays(15));
    scheduler.runDeleteOldDeactivatedExecutions();
    tester.assertNoExecution(INSTANCE);
  }

  private ManualScheduler createManualScheduler(OneTimeTask<Void> task) {
    return TestHelper.createManualScheduler(postgres.getDataSource(), task).clock(clock).build();
  }
}
