/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public interface FailureHandler<T> {

    void onFailure(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations);

    // TODO: failure handler with backoff
    class OnFailureRetryLater<T> implements FailureHandler<T> {

        private static final Logger LOG = LoggerFactory.getLogger(CompletionHandler.OnCompleteReschedule.class);
        private Duration sleepDuration;

        public OnFailureRetryLater(Duration sleepDuration) {
            this.sleepDuration = sleepDuration;
        }

        @Override
        public void onFailure(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
            Instant nextTry = Instant.now().plus(sleepDuration);
            LOG.debug("Execution failed. Retrying task {} at {}", executionComplete.getExecution().taskInstance, nextTry);
            executionOperations.reschedule(executionComplete, nextTry);
        }
    }

    class OnFailureReschedule<T> implements FailureHandler<T> {

        private static final Logger LOG = LoggerFactory.getLogger(CompletionHandler.OnCompleteReschedule.class);
        private final Schedule schedule;

        public OnFailureReschedule(Schedule schedule) {
            this.schedule = schedule;
        }

        @Override
        public void onFailure(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
            Instant nextExecution = schedule.getNextExecutionTime(executionComplete.getTimeDone());
            LOG.debug("Execution failed. Rescheduling task {} to {}", executionComplete.getExecution().taskInstance, nextExecution);
            executionOperations.reschedule(executionComplete, nextExecution);
        }
    }
}
