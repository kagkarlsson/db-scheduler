package com.github.kagkarlsson.scheduler;

import java.time.Instant;

public final class TaskEntityBuilder {

    private String taskName;
    private String taskInstance;
    private byte[] taskData;
    private Instant executionTime;
    private boolean picked;
    private String pickedBy;
    private Instant lastSuccess;
    private Instant lastFailure;
    private int consecutiveFailures;
    private Instant lastHeartbeat;
    private long version;

    private TaskEntityBuilder() {
    }

    public static TaskEntityBuilder aTaskEntity() {
        return new TaskEntityBuilder();
    }

    public TaskEntityBuilder taskName(String taskName) {
        this.taskName = taskName;
        return this;
    }

    public TaskEntityBuilder taskInstance(String taskInstance) {
        this.taskInstance = taskInstance;
        return this;
    }

    public TaskEntityBuilder taskData(byte[] taskData) {
        this.taskData = taskData;
        return this;
    }

    public TaskEntityBuilder executionTime(Instant executionTime) {
        this.executionTime = executionTime;
        return this;
    }

    public TaskEntityBuilder picked(boolean picked) {
        this.picked = picked;
        return this;
    }

    public TaskEntityBuilder pickedBy(String pickedBy) {
        this.pickedBy = pickedBy;
        return this;
    }

    public TaskEntityBuilder lastSuccess(Instant lastSuccess) {
        this.lastSuccess = lastSuccess;
        return this;
    }

    public TaskEntityBuilder lastFailure(Instant lastFailure) {
        this.lastFailure = lastFailure;
        return this;
    }

    public TaskEntityBuilder consecutiveFailures(int consecutiveFailures) {
        this.consecutiveFailures = consecutiveFailures;
        return this;
    }

    public TaskEntityBuilder lastHeartbeat(Instant lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
        return this;
    }

    public TaskEntityBuilder version(long version) {
        this.version = version;
        return this;
    }

    public TaskEntity build() {
        TaskEntity taskEntity = new TaskEntity();
        taskEntity.setTaskName(taskName);
        taskEntity.setTaskInstance(taskInstance);
        taskEntity.setTaskData(taskData);
        taskEntity.setExecutionTime(executionTime);
        taskEntity.setPicked(picked);
        taskEntity.setPickedBy(pickedBy);
        taskEntity.setLastSuccess(lastSuccess);
        taskEntity.setLastFailure(lastFailure);
        taskEntity.setConsecutiveFailures(consecutiveFailures);
        taskEntity.setLastHeartbeat(lastHeartbeat);
        taskEntity.setVersion(version);
        return taskEntity;
    }
}
