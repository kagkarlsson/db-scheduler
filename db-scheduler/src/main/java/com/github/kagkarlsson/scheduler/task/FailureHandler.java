/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static java.lang.Math.pow;
import static java.lang.Math.round;

public interface FailureHandler<T> {

    void onFailure(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations);

    class ExponentialBackoffFailureHandler<T> implements FailureHandler<T> {
        private static final Logger LOG = LoggerFactory.getLogger(ExponentialBackoffFailureHandler.class);
        private static final double DEFAULT_MULTIPLIER = 1.5;
        private final Duration sleepDuration;
        private final double exponentialRate;

        public ExponentialBackoffFailureHandler(Duration sleepDuration){
            this.sleepDuration = sleepDuration;
            this.exponentialRate = DEFAULT_MULTIPLIER;
        }

        public ExponentialBackoffFailureHandler(Duration sleepDuration, double exponentialRate){
            this.sleepDuration = sleepDuration;
            this.exponentialRate = exponentialRate;
        }

        @Override
        public void onFailure(final ExecutionComplete executionComplete, final ExecutionOperations<T> executionOperations) {
            long retryDurationMs = round(sleepDuration.toMillis() * pow(exponentialRate, executionComplete.getExecution().consecutiveFailures));
            Instant nextTry = Instant.now().plusMillis(retryDurationMs);
            LOG.debug("Execution failed. Retrying task {} at {}", executionComplete.getExecution().taskInstance, nextTry);
            executionOperations.reschedule(executionComplete, nextTry);
        }
    }

    class MaxRetriesFailureHandler<T> implements FailureHandler<T> {
        private static final Logger LOG = LoggerFactory.getLogger(MaxRetriesFailureHandler.class);
        private final int maxRetries;
        private final FailureHandler<T> failureHandler;

        public MaxRetriesFailureHandler(int maxRetries, FailureHandler<T> failureHandler){
            this.maxRetries = maxRetries;
            this.failureHandler = failureHandler;
        }

        @Override
        public void onFailure(final ExecutionComplete executionComplete, final ExecutionOperations<T> executionOperations) {
            if(executionComplete.getExecution().consecutiveFailures >= maxRetries){
                LOG.error("Max execution attempts exceeded, task instance {} will no longer be handled.", executionComplete.getExecution().taskInstance);
                executionOperations.stop();
            }else{
                this.failureHandler.onFailure(executionComplete, executionOperations);
            }
        }
    }

    class OnFailureRetryLater<T> implements FailureHandler<T> {

        private static final Logger LOG = LoggerFactory.getLogger(CompletionHandler.OnCompleteReschedule.class);
        private final Duration sleepDuration;

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
            Instant nextExecution = schedule.getNextExecutionTime(executionComplete);
            LOG.debug("Execution failed. Rescheduling task {} to {}", executionComplete.getExecution().taskInstance, nextExecution);
            executionOperations.reschedule(executionComplete, nextExecution);
        }
    }
}
