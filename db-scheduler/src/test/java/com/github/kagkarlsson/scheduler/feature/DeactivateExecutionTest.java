package com.github.kagkarlsson.scheduler.feature;

import static com.github.kagkarlsson.scheduler.TestTasks.DO_NOTHING;
import static com.github.kagkarlsson.scheduler.TestTasks.EVERY_FIVE_SEC;
import static com.github.kagkarlsson.scheduler.TestTasks.ONETIME;
import static com.github.kagkarlsson.scheduler.TestTasks.ON_EXECUTE_THROW;
import static com.github.kagkarlsson.scheduler.TestTasks.RECURRING;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.SchedulerTester;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceNotActiveException;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceNotFoundException;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class DeactivateExecutionTest {

  private final SettableClock clock = new SettableClock();
  private final Instant anFutureInstant = clock.now().plusSeconds(10);

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @Test
  public void should_deactivate_scheduled_execution() {
    var task = Tasks.recurring(RECURRING, EVERY_FIVE_SEC).execute(DO_NOTHING);
    var scheduler = createManualScheduler(task);
    var tester = new SchedulerTester(scheduler);

    scheduler.runOnStartup();
    scheduler.runAnyDueExecutions();

    TaskInstanceId instance = RECURRING.instanceId(RecurringTask.INSTANCE);
    scheduler.deactivate(instance, State.PAUSED);

    tester.assertThatExecution(instance).hasState(State.PAUSED).hasLastSuccess(clock.now());
  }

  @Test
  public void should_keep_consecutive_failures_when_deactivating() {
    var task = Tasks.oneTime(ONETIME).execute(ON_EXECUTE_THROW);
    var scheduler = createManualScheduler(task);
    var tester = new SchedulerTester(scheduler);

    var scheduled = ONETIME.instance("1").scheduledTo(clock.now());
    scheduler.schedule(scheduled);
    scheduler.runAnyDueExecutions();

    scheduler.deactivate(scheduled, State.FAILED);
    tester
        .assertThatExecution(scheduled)
        .hasState(State.FAILED)
        .hasConsecutiveFailures(1)
        .hasLastFailure(clock.now());
  }

  @Test
  public void should_throw_when_execution_not_found() {
    var scheduler = createManualScheduler(Tasks.oneTime(ONETIME).execute(DO_NOTHING));

    var nonExistentInstance = ONETIME.instanceId("non-existent");

    assertThatThrownBy(() -> scheduler.deactivate(nonExistentInstance, State.PAUSED))
        .isInstanceOf(TaskInstanceNotFoundException.class);
  }

  @Test
  public void should_throw_when_execution_is_already_deactivated() {
    var scheduler = createManualScheduler(Tasks.oneTime(ONETIME).execute(DO_NOTHING));

    var instance = ONETIME.instance("1").scheduledTo(anFutureInstant);
    scheduler.schedule(instance);
    scheduler.deactivate(instance, State.PAUSED);

    assertThatThrownBy(() -> scheduler.deactivate(instance, State.PAUSED))
        .isInstanceOf(TaskInstanceNotActiveException.class);
  }

  private ManualScheduler createManualScheduler(Task<Void> task) {
    if (task instanceof RecurringTask<Void>) {
      return TestHelper.createManualScheduler(postgres.getDataSource())
          .clock(clock)
          .startTasks(List.of((RecurringTask<Void>) task))
          .build();
    } else {
      return TestHelper.createManualScheduler(postgres.getDataSource(), task).clock(clock).build();
    }
  }
}
