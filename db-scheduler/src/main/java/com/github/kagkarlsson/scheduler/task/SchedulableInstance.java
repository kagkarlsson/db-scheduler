package com.github.kagkarlsson.scheduler.task;

import java.time.Instant;
import java.util.function.Supplier;

public interface SchedulableInstance<T> extends TaskInstanceId {

    TaskInstance<T> getTaskInstance();
    Instant getExecutionTime();

    default String getTaskName() {
        return getTaskInstance().getTaskName();
    }

    default String getId() {
        return getTaskInstance().getId();
    }

    //    SchedulableBuilder of(TaskInstanceId)
//

//    default SchedulableBuilder of(TaskInstanceId instance)
//    class SchedulableBuilder implements Schedulable {
//
//
//    }

    class SchedulableTaskInstance<T> implements SchedulableInstance<T> {
        private final TaskInstance<T> taskInstance;
        Supplier<Instant> executionTime;

        public SchedulableTaskInstance(TaskInstance<T> taskInstance, Supplier<Instant> executionTime) {
            this.taskInstance = taskInstance;
            this.executionTime = executionTime;
        }

        @Override
        public TaskInstance<T> getTaskInstance() {
            return taskInstance;
        }

        @Override
        public Instant getExecutionTime() {
            return executionTime.get();
        }
    }


}
