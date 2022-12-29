package com.github.kagkarlsson.scheduler.task;

/**
 * Experimental
 */
public class TaskWithoutDataDescriptor implements TaskDescriptor<Void> {

    private final String taskName;

    public TaskWithoutDataDescriptor(String taskName) {
        this.taskName = taskName;
    }

    public TaskInstance<Void> instance(String id) {
        return new TaskInstance<>(taskName, id);
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public Class<Void> getDataClass() {
        return Void.class;
    }

    public TaskInstanceId instanceId(String id) {
        return TaskInstanceId.of(taskName, id);
    }
}
