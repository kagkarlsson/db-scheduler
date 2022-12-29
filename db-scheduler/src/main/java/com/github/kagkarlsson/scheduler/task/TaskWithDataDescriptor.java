package com.github.kagkarlsson.scheduler.task;

/**
 * Experimental
 */
public class TaskWithDataDescriptor<T> implements HasTaskName {

    private final String taskName;
    private final Class<?> dataClass;

    public TaskWithDataDescriptor(String taskName, Class<?> dataClass) { //TODO: not used?
        this.taskName = taskName;
        this.dataClass = dataClass;
    }

    public TaskInstance<T> instance(String id, T data) {
        return new TaskInstance<>(taskName, id, data);
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    public TaskInstanceId instanceId(String id) {
        return TaskInstanceId.of(taskName, id);
    }
}
