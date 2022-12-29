package com.github.kagkarlsson.scheduler.task;

/**
 * Experimental
 */
public class TaskDescriptor implements HasTaskName {

    private final String taskName;

    public TaskDescriptor(String taskName) {
        this.taskName = taskName;
    }

    public TaskInstance<Void> instance(String id) {
        return new TaskInstance<>(taskName, id);
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    public TaskInstanceId instanceId(String id) {
        return TaskInstanceId.of(taskName, id);
    }
}
