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

import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public interface CompletionHandler<T> {

    void complete(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations);


    class OnCompleteRemove<T> implements CompletionHandler<T> {

        @Override
        public void complete(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
            executionOperations.stop();
        }
    }

    class OnCompleteReschedule<T> implements CompletionHandler<T> {
        private static final Logger LOG = LoggerFactory.getLogger(OnCompleteReschedule.class);
        private final Schedule schedule;
        private final boolean setNewData;
        private T newData;

        public OnCompleteReschedule(Schedule schedule) {
            this.schedule = schedule;
            this.setNewData = false;
        }

        public OnCompleteReschedule(Schedule schedule, T newData) {
            this.schedule = schedule;
            this.newData = newData;
            this.setNewData = true;
        }

        @Override
        public void complete(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
            Instant nextExecution = schedule.getNextExecutionTime(executionComplete);
            LOG.debug("Rescheduling task {} to {}", executionComplete.getExecution().taskInstance, nextExecution);
            if (setNewData) {
                executionOperations.reschedule(executionComplete, nextExecution, newData);
            } else {
                executionOperations.reschedule(executionComplete, nextExecution);
            }
        }

        @Override
        public String toString() {
            return "OnCompleteReschedule with " + schedule;
        }

    }

}
