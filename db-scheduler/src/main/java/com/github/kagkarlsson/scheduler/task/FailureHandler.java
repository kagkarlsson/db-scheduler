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
package com.github.kagkarlsson.scheduler.task;

import static java.lang.Math.pow;
import static java.lang.Math.round;

import com.github.kagkarlsson.scheduler.jdbc.DeactivationUpdate;
import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface FailureHandler<T> {

  void onFailure(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations);

  /**
   * Callback invoked when max retries have been exceeded and the execution has been
   * removed/deactivated.
   */
  @FunctionalInterface
  interface MaxRetriesExceededCallback {
    void maxRetriesExceeded(ExecutionComplete executionComplete);
  }

  class ExponentialBackoffFailureHandler<T> implements FailureHandler<T> {
    private static final Logger LOG =
        LoggerFactory.getLogger(ExponentialBackoffFailureHandler.class);
    private static final double DEFAULT_MULTIPLIER = 1.5;
    private final Duration sleepDuration;
    private final double exponentialRate;

    public ExponentialBackoffFailureHandler(Duration sleepDuration) {
      this.sleepDuration = sleepDuration;
      this.exponentialRate = DEFAULT_MULTIPLIER;
    }

    public ExponentialBackoffFailureHandler(Duration sleepDuration, double exponentialRate) {
      this.sleepDuration = sleepDuration;
      this.exponentialRate = exponentialRate;
    }

    @Override
    public void onFailure(
        final ExecutionComplete executionComplete,
        final ExecutionOperations<T> executionOperations) {
      long retryDurationMs =
          round(
              sleepDuration.toMillis()
                  * pow(exponentialRate, executionComplete.getExecution().consecutiveFailures));
      Instant nextTry = executionComplete.getTimeDone().plusMillis(retryDurationMs);
      LOG.debug(
          "Execution failed {}. Retrying task {} at {}",
          executionComplete.getTimeDone(),
          executionComplete.getExecution().taskInstance,
          nextTry);
      executionOperations.reschedule(executionComplete, nextTry);
    }
  }

  /**
   * @deprecated Use {@link #maxRetries(int)} builder instead.
   */
  @Deprecated
  class MaxRetriesFailureHandler<T> implements FailureHandler<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MaxRetriesFailureHandler.class);
    private final int maxRetries;
    private final FailureHandler<T> failureHandler;
    private final BiConsumer<ExecutionComplete, ExecutionOperations<T>> maxRetriesExceededHandler;

    public MaxRetriesFailureHandler(int maxRetries, FailureHandler<T> failureHandler) {
      this(maxRetries, failureHandler, (executionComplete, executionOperations) -> {});
    }

    public MaxRetriesFailureHandler(
        int maxRetries,
        FailureHandler<T> failureHandler,
        BiConsumer<ExecutionComplete, ExecutionOperations<T>> maxRetriesExceededHandler) {
      this.maxRetries = maxRetries;
      this.failureHandler = failureHandler;
      this.maxRetriesExceededHandler = maxRetriesExceededHandler;
    }

    @Override
    public void onFailure(
        final ExecutionComplete executionComplete,
        final ExecutionOperations<T> executionOperations) {
      int consecutiveFailures = executionComplete.getExecution().consecutiveFailures;
      int totalNumberOfFailures = consecutiveFailures + 1;
      if (totalNumberOfFailures > maxRetries) {
        LOG.error(
            "Execution has failed {} times for task instance {}. Cancelling execution.",
            totalNumberOfFailures,
            executionComplete.getExecution().taskInstance);
        executionOperations.remove();
        maxRetriesExceededHandler.accept(executionComplete, executionOperations);
      } else {
        this.failureHandler.onFailure(executionComplete, executionOperations);
      }
    }
  }

  /** Builder for creating max-retries failure handlers with a fluent API. */
  class MaxRetriesBuilder<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MaxRetriesBuilder.class);
    private final int maxRetries;
    private FailureHandler<T> retryHandler;

    private MaxRetriesBuilder(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    /** Retry with exponential backoff. */
    public MaxRetriesBuilder<T> withBackoff(Duration initialDelay, double multiplier) {
      this.retryHandler = new ExponentialBackoffFailureHandler<>(initialDelay, multiplier);
      return this;
    }

    /** Retry with exponential backoff using the default multiplier (1.5). */
    public MaxRetriesBuilder<T> withBackoff(Duration initialDelay) {
      this.retryHandler = new ExponentialBackoffFailureHandler<>(initialDelay);
      return this;
    }

    /** Retry with a fixed delay between attempts. */
    public MaxRetriesBuilder<T> retryEvery(Duration delay) {
      this.retryHandler = new OnFailureRetryLater<>(delay);
      return this;
    }

    /** After max retries, remove the execution. */
    public FailureHandler<T> thenRemove() {
      return thenRemove(complete -> {});
    }

    /** After max retries, remove the execution and call the callback. */
    public FailureHandler<T> thenRemove(MaxRetriesExceededCallback callback) {
      return then(
          (complete, ops) -> {
            LOG.error(
                "Execution has failed max retries for task instance {}. Removing.",
                complete.getExecution().taskInstance);
            ops.remove();
            callback.maxRetriesExceeded(complete);
          });
    }

    /** After max retries, deactivate with the specified state. */
    public FailureHandler<T> thenDeactivate(State state) {
      return thenDeactivate(state, complete -> {});
    }

    /** After max retries, deactivate with the specified state and call the callback. */
    public FailureHandler<T> thenDeactivate(State state, MaxRetriesExceededCallback callback) {
      return then(
          (complete, ops) -> {
            LOG.error(
                "Execution has failed max retries for task instance {}. Deactivating with state {}.",
                complete.getExecution().taskInstance,
                state);
            ops.deactivate(
                DeactivationUpdate.toState(state).lastFailed(complete.getTimeDone()).build());
            callback.maxRetriesExceeded(complete);
          });
    }

    /**
     * After max retries, call the callback with full control (no automatic remove/deactivate). The
     * callback should call one of the ExecutionOperations methods (remove, deactivate, or
     * reschedule).
     */
    public FailureHandler<T> then(BiConsumer<ExecutionComplete, ExecutionOperations<T>> callback) {
      FailureHandler<T> retryHandler = getRetryHandler();
      return (executionComplete, executionOperations) -> {
        int totalFailures = executionComplete.getExecution().consecutiveFailures + 1;
        if (totalFailures > maxRetries) {
          callback.accept(executionComplete, executionOperations);
        } else {
          retryHandler.onFailure(executionComplete, executionOperations);
        }
      };
    }

    private FailureHandler<T> getRetryHandler() {
      if (retryHandler == null) {
        throw new IllegalStateException(
            "Must specify retry behavior (withBackoff or retryEvery) before terminal operation");
      }
      return retryHandler;
    }
  }

  /** Start building a max-retries failure handler. */
  static <T> MaxRetriesBuilder<T> maxRetries(int maxRetries) {
    return new MaxRetriesBuilder<>(maxRetries);
  }

  class OnFailureRetryLater<T> implements FailureHandler<T> {
    private static final Logger LOG =
        LoggerFactory.getLogger(CompletionHandler.OnCompleteReschedule.class);
    private final Duration sleepDuration;

    public OnFailureRetryLater(Duration sleepDuration) {
      this.sleepDuration = sleepDuration;
    }

    @Override
    public void onFailure(
        ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
      Instant nextTry = executionComplete.getTimeDone().plus(sleepDuration);
      LOG.debug(
          "Execution failed. Retrying task {} at {}",
          executionComplete.getExecution().taskInstance,
          nextTry);
      executionOperations.reschedule(executionComplete, nextTry);
    }
  }

  class OnFailureReschedule<T> implements FailureHandler<T> {
    private static final Logger LOG =
        LoggerFactory.getLogger(CompletionHandler.OnCompleteReschedule.class);
    private final Schedule schedule;

    public OnFailureReschedule(Schedule schedule) {
      this.schedule = schedule;
    }

    @Override
    public void onFailure(
        ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
      Instant nextExecution = schedule.getNextExecutionTime(executionComplete);
      LOG.debug(
          "Execution failed. Rescheduling task {} to {}",
          executionComplete.getExecution().taskInstance,
          nextExecution);
      executionOperations.reschedule(executionComplete, nextExecution);
    }
  }

  @SuppressWarnings("unchecked")
  class OnFailureRescheduleUsingTaskDataSchedule<T extends ScheduleAndData>
      implements FailureHandler<T> {
    private static final Logger LOG =
        LoggerFactory.getLogger(CompletionHandler.OnCompleteReschedule.class);

    @Override
    public void onFailure(
        ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
      final T data = (T) executionComplete.getExecution().taskInstance.getData();
      final Instant nextExecutionTime = data.getSchedule().getNextExecutionTime(executionComplete);
      LOG.debug(
          "Execution failed. Rescheduling task {} to {}",
          executionComplete.getExecution().taskInstance,
          nextExecutionTime);
      executionOperations.reschedule(executionComplete, nextExecutionTime);
    }
  }
}
