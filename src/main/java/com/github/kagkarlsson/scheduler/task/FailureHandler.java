package com.github.kagkarlsson.scheduler.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public interface FailureHandler {

    void onFailure(ExecutionFailed executionComplete, ExecutionOperations executionOperations);

    class OnFailureRetryLater implements FailureHandler {

        private static final Logger LOG = LoggerFactory.getLogger(CompletionHandler.OnCompleteReschedule.class);
        private Duration sleepDuration;

        public OnFailureRetryLater(Duration sleepDuration) {
            this.sleepDuration = sleepDuration;
        }

        @Override
        public void onFailure(ExecutionFailed executionComplete, ExecutionOperations executionOperations) {
            Instant nextTry = Instant.now().plus(sleepDuration);
            LOG.debug("Execution failed. Retrying task {} at {}", executionComplete.getExecution().taskInstance, nextTry);
            executionOperations.reschedule(executionComplete, nextTry);
        }
    }

    // TODO: failure handler with backoff
    class OnFailureReschedule implements FailureHandler {

        private static final Logger LOG = LoggerFactory.getLogger(CompletionHandler.OnCompleteReschedule.class);
        private final Schedule schedule;

        public OnFailureReschedule(Schedule schedule) {
            this.schedule = schedule;
        }

        @Override
        public void onFailure(ExecutionFailed executionComplete, ExecutionOperations executionOperations) {
            Instant nextExecution = schedule.getNextExecutionTime(executionComplete.getTimeDone());
            LOG.debug("Execution failed. Rescheduling task {} to {}", executionComplete.getExecution().taskInstance, nextExecution);
            executionOperations.reschedule(executionComplete, nextExecution);
        }
    }
}
