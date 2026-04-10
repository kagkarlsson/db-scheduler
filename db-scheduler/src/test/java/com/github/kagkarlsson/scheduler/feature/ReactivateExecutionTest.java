package com.github.kagkarlsson.scheduler.feature;

import static com.github.kagkarlsson.scheduler.TestTasks.DO_NOTHING;
import static com.github.kagkarlsson.scheduler.TestTasks.ONETIME;
import static com.github.kagkarlsson.scheduler.TestTasks.ON_EXECUTE_THROW;
import static com.github.kagkarlsson.scheduler.TestTasks.ON_FAILURE_DEACTIVATE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.SchedulerTester;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceNotDeactivatedException;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceNotFoundException;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ReactivateExecutionTest {

  private final SettableClock clock = new SettableClock();
  private final Instant anFutureInstant = clock.now().plusSeconds(10);

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @Test
  public void should_reactivate_failed_execution() {
    var failingTask =
        Tasks.oneTime(ONETIME).onFailure(ON_FAILURE_DEACTIVATE).execute(ON_EXECUTE_THROW);
    var scheduler = createManualScheduler(failingTask);
    var tester = new SchedulerTester(scheduler);

    var scheduled = ONETIME.instance("1").scheduledTo(clock.now());
    scheduler.schedule(scheduled);
    scheduler.runAnyDueExecutions();

    tester.assertThatExecution(scheduled).hasState(State.FAILED);

    // Reactivate
    scheduler.reactivate(scheduled, anFutureInstant);

    tester.assertThatExecution(scheduled).hasState(State.ACTIVE).hasExecutionTime(anFutureInstant);
  }

  @Test
  public void should_throw_when_execution_not_found() {
    var scheduler = createManualScheduler(Tasks.oneTime(ONETIME).execute(DO_NOTHING));

    var nonExistentInstance = ONETIME.instanceId("non-existent");

    assertThatThrownBy(() -> scheduler.reactivate(nonExistentInstance, anFutureInstant))
        .isInstanceOf(TaskInstanceNotFoundException.class);
  }

  @Test
  public void should_throw_when_execution_is_active() {
    var scheduler = createManualScheduler(Tasks.oneTime(ONETIME).execute(DO_NOTHING));

    var instance = ONETIME.instance("1").scheduledTo(anFutureInstant);
    scheduler.schedule(instance);

    assertThatThrownBy(() -> scheduler.reactivate(instance, anFutureInstant))
        .isInstanceOf(TaskInstanceNotDeactivatedException.class);
  }

  private ManualScheduler createManualScheduler(OneTimeTask<Void> task) {
    return TestHelper.createManualScheduler(postgres.getDataSource(), task).clock(clock).build();
  }
}
