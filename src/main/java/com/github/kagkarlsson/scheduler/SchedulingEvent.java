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
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.TaskInstanceId;

import java.time.Instant;

public class SchedulingEvent {

    enum EventType {
        SCHEDULE,
        RESCHEDULE,
        CANCEL
    }

    private SchedulingEventContext ctx;

    public SchedulingEvent(SchedulingEventContext ctx) {
        this.ctx = ctx;
    }

    public SchedulingEventContext getContext() {
        return ctx;
    }

    public static class SchedulingEventContext {
        private final EventType eventType;
        private final TaskInstanceId taskInstanceId;
        private final Instant executionTime;

        public SchedulingEventContext(EventType eventType, TaskInstanceId taskInstanceId, Instant executionTime) {
            this.eventType = eventType;
            this.taskInstanceId = taskInstanceId;
            this.executionTime = executionTime;
        }

        public EventType getEventType() {
            return eventType;
        }

        public TaskInstanceId getTaskInstanceId() {
            return taskInstanceId;
        }

        public Instant getExecutionTime() {
            return executionTime;
        }
    }
}
