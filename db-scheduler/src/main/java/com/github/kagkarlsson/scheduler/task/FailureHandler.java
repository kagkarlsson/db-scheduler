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

import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface FailureHandler<T> {

  void onFailure(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations);

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
        executionOperations.stop();
        maxRetriesExceededHandler.accept(executionComplete, executionOperations);
      } else {
        this.failureHandler.onFailure(executionComplete, executionOperations);
      }
    }
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

  /**
   * Failure handler to create a sequence of failure handlers
   *
   * <p>The SequenceFailureHandler facilitates the construction of intricate failure handling
   * mechanisms. It allows for behaviors such as retrying an operation up to five times with
   * specified delays between attempts, and if unsuccessful, automatically rescheduling the task for
   * the following day to maintain operational continuity.
   */
  class SequenceFailureHandler<T> implements FailureHandler<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceFailureHandler.class);
    private final FailureHandler<T> primaryFailureHandler;
    private final FailureHandler<T> secondaryFailureHandler;
    private final int primaryHandlerRetryCount;

    /**
     * Create a sequence of failure handlers. The primary failure handler is used for the first
     * failure. If the execution fails again, the secondary failure handler is used.
     *
     * <p>The PrimaryFailureHandler cannot be a MaxRetriesFailureHandler as it stops the execution.
     * Use the primaryHandlerRetryCount parameter instead if retries are needed.
     *
     * @param primaryFailureHandler The primary failure handler. Cannot be a
     *     MaxRetriesFailureHandler
     * @param secondaryFailureHandler The secondary failure handler
     */
    public SequenceFailureHandler(
        FailureHandler<T> primaryFailureHandler, FailureHandler<T> secondaryFailureHandler) {
      this(primaryFailureHandler, secondaryFailureHandler, 1);
    }

    /**
     * Create a sequence of failure handlers. The primary failure handler is used for failures until
     * the primaryHandlerRetryCount is reached. If the execution fails again, the secondary failure
     * handler is used.
     *
     * <p>For example, if primaryHandlerRetryCount is 2, the primary failure handler will be used
     * for the first two failures. If the execution fails a third time, the secondary failure
     * handler is used.
     *
     * <p>The PrimaryFailureHandler cannot be a MaxRetriesFailureHandler as it stops the execution.
     * Use the primaryHandlerRetryCount parameter instead if retries are needed.
     *
     * <p>Primary handler retry count must be at least 1.
     *
     * @param primaryFailureHandler The primary failure handler. Cannot be a
     *     MaxRetriesFailureHandler
     * @param secondaryFailureHandler The secondary failure handler
     * @param primaryHandlerRetryCount The number of retries to use the primary failure handler.
     *     Must be at least 1.
     */
    public SequenceFailureHandler(
        FailureHandler<T> primaryFailureHandler,
        FailureHandler<T> secondaryFailureHandler,
        int primaryHandlerRetryCount) {
      if (primaryFailureHandler instanceof MaxRetriesFailureHandler) {
        throw new IllegalArgumentException(
            "Primary failure handler cannot be a MaxRetriesFailureHandler as it stops the execution. Use the primaryHandlerRetryCount parameter instead if retries are needed.");
      }
      this.primaryFailureHandler = primaryFailureHandler;
      this.secondaryFailureHandler = secondaryFailureHandler;
      if (primaryHandlerRetryCount < 1) {
        throw new IllegalArgumentException("Primary handler retry count must be at least 1.");
      }
      this.primaryHandlerRetryCount = primaryHandlerRetryCount;
    }

    @Override
    public void onFailure(
        ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
      int consecutiveFailures = executionComplete.getExecution().consecutiveFailures;
      int totalNumberOfFailures = consecutiveFailures + 1;
      if (totalNumberOfFailures > primaryHandlerRetryCount) {
        LOG.debug(
            "Primary handler retry count exceeded for task instance {}. Switching to secondary failure handler.",
            executionComplete.getExecution().taskInstance);
        secondaryFailureHandler.onFailure(executionComplete, executionOperations);
      } else {
        LOG.debug(
            "Using primary failure handler for task instance {} with {} failures.",
            executionComplete.getExecution().taskInstance,
            totalNumberOfFailures);
        primaryFailureHandler.onFailure(executionComplete, executionOperations);
      }
    }

    public static <T> SequenceFailureHandler<T> of(
        FailureHandler<T> primaryFailureHandler, FailureHandler<T> secondaryFailureHandler) {
      return SequenceFailureHandler.of(primaryFailureHandler, secondaryFailureHandler, 1);
    }

    public static <T> SequenceFailureHandler<T> of(
        FailureHandler<T> primaryFailureHandler,
        FailureHandler<T> secondaryFailureHandler,
        int primaryHandlerRetryCount) {
      return new SequenceFailureHandler<>(
          primaryFailureHandler, secondaryFailureHandler, primaryHandlerRetryCount);
    }

    public static <T> SequenceFailureHandlerBuilder<T> builder() {
      return new SequenceFailureHandlerBuilder<>();
    }

    public static class SequenceFailureHandlerBuilder<T> {
      private FailureHandler<T> primaryFailureHandler;
      private FailureHandler<T> secondaryFailureHandler;
      private int primaryHandlerRetryCount;

      public SequenceFailureHandlerBuilder() {
        this(null, null, 1);
      }

      public SequenceFailureHandlerBuilder(
          FailureHandler<T> primaryFailureHandler, FailureHandler<T> secondaryFailureHandler) {
        this(primaryFailureHandler, secondaryFailureHandler, 1);
      }

      public SequenceFailureHandlerBuilder(
          FailureHandler<T> primaryFailureHandler,
          FailureHandler<T> secondaryFailureHandler,
          int primaryHandlerRetryCount) {
        this.primaryFailureHandler = primaryFailureHandler;
        this.secondaryFailureHandler = secondaryFailureHandler;
        this.primaryHandlerRetryCount = primaryHandlerRetryCount;
      }

      public SequenceFailureHandlerBuilder<T> primary(FailureHandler<T> primaryFailureHandler) {
        this.primaryFailureHandler = primaryFailureHandler;
        return this;
      }

      public SequenceFailureHandlerBuilder<T> secondary(FailureHandler<T> secondaryFailureHandler) {
        this.secondaryFailureHandler = secondaryFailureHandler;
        return this;
      }

      public SequenceFailureHandlerBuilder<T> afterTries(
          int primaryHandlerRetryCount, FailureHandler<T> secondaryFailureHandler) {
        this.secondaryFailureHandler = secondaryFailureHandler;
        this.primaryHandlerRetryCount = primaryHandlerRetryCount;
        return this;
      }

      public SequenceFailureHandler<T> build() {
        return new SequenceFailureHandler<>(
            primaryFailureHandler, secondaryFailureHandler, primaryHandlerRetryCount);
      }
    }
  }
}
