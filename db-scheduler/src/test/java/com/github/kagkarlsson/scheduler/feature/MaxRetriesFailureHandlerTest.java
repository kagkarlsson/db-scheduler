/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.feature;

import static com.github.kagkarlsson.scheduler.TestTasks.ONETIME;
import static com.github.kagkarlsson.scheduler.TestTasks.ON_EXECUTE_THROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.SchedulerTester;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.MaxRetriesExceededListener;
import com.github.kagkarlsson.scheduler.task.RescheduleUpdate;
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
    assertThat(callbackTracker.invoked).isFalse();

    runTimes(1);
    tester.assertNoExecution(instance);
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
  void maxRetries_then_callback_should_run_when_max_exceeded() {
    var tomorrow = clock.now().plus(Duration.ofDays(1));
    var instance =
        setupSchedulerAndFailingTask(
            FailureHandler.<Void>maxRetries(2)
                .retryEvery(RETRY_INTERVAL)
                .then(
                    (complete, ops) -> {
                      callbackTracker.invoked = true;
                      ops.reschedule(complete, tomorrow);
                    }));

    runTimes(3);

    tester.assertThatExecution(instance).isScheduled().hasExecutionTime(tomorrow);
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
                        ops.reschedule(
                            RescheduleUpdate.toExecutionTime(tomorrow).resetFailures().build())));

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
                            RescheduleUpdate.toExecutionTime(tomorrow)
                                .consecutiveFailures(10)
                                .build())));

    runTimes(3);

    tester.assertThatExecution(instance).isScheduled().hasConsecutiveFailures(10);
  }

  @Test
  void deprecated_MaxRetriesFailureHandler_should_still_work() {
    var instance =
        setupSchedulerAndFailingTask(
            new FailureHandler.MaxRetriesFailureHandler<>(
                3,
                new FailureHandler.OnFailureRetryLater<>(RETRY_INTERVAL),
                (complete, ops) -> callbackTracker.invoked = true));

    runTimes(3);
    tester.assertThatExecution(instance).isScheduled();

    runTimes(1);
    tester.assertNoExecution(instance);
    assertThat(callbackTracker.invoked).isTrue();
  }

  @Test
  void builder_should_throw_when_no_retry_strategy_set() {
    assertThatThrownBy(() -> FailureHandler.<Void>maxRetries(3).thenRemove())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("retry behavior");
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

  private static class CallbackTracker implements MaxRetriesExceededListener {
    boolean invoked = false;

    @Override
    public void onMaxRetriesExceeded(ExecutionComplete executionComplete) {
      invoked = true;
    }
  }
}
