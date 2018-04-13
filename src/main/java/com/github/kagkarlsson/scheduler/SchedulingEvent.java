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
