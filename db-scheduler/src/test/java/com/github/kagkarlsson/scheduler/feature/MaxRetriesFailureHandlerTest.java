package com.github.kagkarlsson.scheduler.feature;

import static com.github.kagkarlsson.scheduler.TestTasks.ONETIME;
import static com.github.kagkarlsson.scheduler.TestTasks.ON_EXECUTE_THROW;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.SchedulerTester;
import com.github.kagkarlsson.scheduler.jdbc.RescheduleUpdate;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.FailureHandler.MaxRetriesExceededCallback;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class MaxRetriesFailureHandlerTest {

  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
  private static final Duration TICK = Duration.ofSeconds(2);

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  private SettableClock clock;
  private ManualScheduler scheduler;
  private SchedulerTester tester;
  private CallbackTracker callbackTracker;

  @BeforeEach
  void setUp() {
    clock = new SettableClock();
    callbackTracker = new CallbackTracker();
  }

  @Test
  void maxRetries_thenRemove_should_remove_after_max_failures() {
    var instance =
        setupSchedulerAndFailingTask(
            FailureHandler.<Void>maxRetries(3)
                .retryEvery(RETRY_INTERVAL)
                .thenRemove(callbackTracker));

    runTimes(3);
    tester.assertThatExecution(instance).isScheduled();

    runTimes(1);
    tester.assertThatExecution(instance).isRemoved();
    assertThat(callbackTracker.invoked).isTrue();
  }

  @Test
  void maxRetries_thenDeactivate_should_deactivate_with_state_after_max_failures() {
    var instance =
        setupSchedulerAndFailingTask(
            FailureHandler.<Void>maxRetries(2)
                .retryEvery(RETRY_INTERVAL)
                .thenDeactivate(State.FAILED, callbackTracker));

    runTimes(2);
    tester.assertThatExecution(instance).isScheduled();

    runTimes(1);
    tester.assertThatExecution(instance).hasState(State.FAILED);
    assertThat(callbackTracker.invoked).isTrue();
  }

  @Test
  void maxRetries_then_should_allow_reschedule_with_reset_failures() {
    var tomorrow = clock.now().plus(Duration.ofDays(1));
    var instance =
        setupSchedulerAndFailingTask(
            FailureHandler.<Void>maxRetries(2)
                .retryEvery(RETRY_INTERVAL)
                .then(
                    (complete, ops) ->
                        ops.reschedule(RescheduleUpdate.to(tomorrow).resetFailures().build())));

    runTimes(3);

    tester
        .assertThatExecution(instance)
        .isScheduled()
        .hasExecutionTime(tomorrow)
        .hasConsecutiveFailures(0)
        .hasNoLastFailure();
  }

  @Test
  void maxRetries_then_should_allow_custom_consecutive_failures() {
    var tomorrow = clock.now().plus(Duration.ofDays(1));
    var instance =
        setupSchedulerAndFailingTask(
            FailureHandler.<Void>maxRetries(2)
                .retryEvery(RETRY_INTERVAL)
                .then(
                    (complete, ops) ->
                        ops.reschedule(
                            RescheduleUpdate.to(tomorrow).consecutiveFailures(10).build())));

    runTimes(3);

    tester.assertThatExecution(instance).isScheduled().hasConsecutiveFailures(10);
  }

  @Test
  void builder_api_should_work_with_thenRemove() {
    var instance =
        setupSchedulerAndFailingTask(
            FailureHandler.<Void>maxRetries(2)
                .retryEvery(RETRY_INTERVAL)
                .thenRemove(callbackTracker));

    runTimes(3);

    tester.assertThatExecution(instance).isRemoved();
    assertThat(callbackTracker.invoked).isTrue();
  }

  @Test
  void builder_api_should_work_with_thenDeactivate() {
    var instance =
        setupSchedulerAndFailingTask(
            FailureHandler.<Void>maxRetries(2)
                .withBackoff(RETRY_INTERVAL, 2.0)
                .thenDeactivate(State.FAILED));

    runTimes(3);

    tester.assertThatExecution(instance).hasState(State.FAILED);
  }

  @Test
  void builder_api_should_work_with_then_callback() {
    var tomorrow = clock.now().plus(Duration.ofDays(1));
    var instance =
        setupSchedulerAndFailingTask(
            FailureHandler.<Void>maxRetries(1)
                .retryEvery(RETRY_INTERVAL)
                .then(
                    (complete, ops) ->
                        ops.reschedule(RescheduleUpdate.to(tomorrow).resetFailures().build())));

    runTimes(2);

    tester
        .assertThatExecution(instance)
        .isScheduled()
        .hasExecutionTime(tomorrow)
        .hasConsecutiveFailures(0);
  }

  private TaskInstanceId setupSchedulerAndFailingTask(FailureHandler<Void> failureHandler) {
    var task = Tasks.oneTime(ONETIME).onFailure(failureHandler).execute(ON_EXECUTE_THROW);
    scheduler =
        TestHelper.createManualScheduler(postgres.getDataSource(), task).clock(clock).build();
    tester = new SchedulerTester(scheduler);
    var instance = ONETIME.instance("1").scheduledTo(clock.now());
    scheduler.schedule(instance);
    return instance;
  }

  private void runTimes(int times) {
    for (int i = 0; i < times; i++) {
      scheduler.runAnyDueExecutions();
      clock.tick(TICK);
    }
  }

  private static class CallbackTracker implements MaxRetriesExceededCallback {
    boolean invoked = false;

    @Override
    public void maxRetriesExceeded(ExecutionComplete executionComplete) {
      invoked = true;
    }
  }
}
