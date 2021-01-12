package com.github.kagkarlsson.scheduler.utils;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.time.Instant;
import java.util.function.Supplier;

public class ExecutionBuilder {

    private Instant executionTime;
    private Instant lastSuccess;
    private Instant lastFailure;
    private Instant lastHeartBeat;
    private byte data[] = {};
    private String taskName;
    private String taskInstanceId;
    private boolean picked;
    private String pickedBy;
    private int consecutiveFailures;
    private long version;

    public ExecutionBuilder executionTime(Instant executionTime) {
        this.executionTime = executionTime;
        return this;
    }

    public ExecutionBuilder lastSuccess(Instant lastSuccess) {
        this.lastSuccess = lastSuccess;
        return this;
    }

    public ExecutionBuilder lastFailure(Instant lastFailure) {
        this.lastFailure = lastFailure;
        return this;
    }

    public ExecutionBuilder lastHeartBeat(Instant lastHeartBeat) {
        this.lastHeartBeat = lastHeartBeat;
        return this;
    }

    public ExecutionBuilder data(byte[] data) {
        this.data = data;
        return this;
    }

    public ExecutionBuilder taskName(String taskName) {
        this.taskName = taskName;
        return this;
    }

    public ExecutionBuilder taskInstanceId(String taskInstanceId) {
        this.taskInstanceId = taskInstanceId;
        return this;
    }

    public ExecutionBuilder picked(boolean picked) {
        this.picked = picked;
        return this;
    }

    public ExecutionBuilder pickedBy(String pickedBy) {
        this.pickedBy = pickedBy;
        return this;
    }

    public ExecutionBuilder consecutiveFailures(int consecutiveFailures) {
        this.consecutiveFailures = consecutiveFailures;
        return this;
    }

    public ExecutionBuilder version(long version) {
        this.version = version;
        return this;
    }

    public ExecutionBuilder() {

    }

    public Execution build() {
        Supplier dataSupplier = () -> data;
        TaskInstance taskInstance = new TaskInstance(taskName, taskInstanceId, dataSupplier);
        return new Execution(executionTime, taskInstance, picked, pickedBy, lastSuccess,
            lastFailure, consecutiveFailures, lastHeartBeat, version);
    }
}
