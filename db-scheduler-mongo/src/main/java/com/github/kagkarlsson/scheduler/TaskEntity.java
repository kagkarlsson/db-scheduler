package com.github.kagkarlsson.scheduler;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

public class TaskEntity {

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

    public static class Fields {
        public static final String taskName = "taskName";
        public static final String taskInstance = "taskInstance";
        public static final String taskData = "taskData";
        public static final String executionTime = "executionTime";
        public static final String picked = "picked";
        public static final String pickedBy = "pickedBy";
        public static final String lastSuccess = "lastSuccess";
        public static final String lastFailure = "lastFailure";
        public static final String consecutiveFailures = "consecutiveFailures";
        public static final String lastHeartbeat = "lastHeartbeat";
        public static final String version = "version";

    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getTaskInstance() {
        return taskInstance;
    }

    public void setTaskInstance(String taskInstance) {
        this.taskInstance = taskInstance;
    }

    public byte[] getTaskData() {
        return taskData;
    }

    public void setTaskData(byte[] taskData) {
        this.taskData = taskData;
    }

    public Instant getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(Instant executionTime) {
        this.executionTime = executionTime;
    }

    public boolean isPicked() {
        return picked;
    }

    public void setPicked(boolean picked) {
        this.picked = picked;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getPickedBy() {
        return pickedBy;
    }

    public void setPickedBy(String pickedBy) {
        this.pickedBy = pickedBy;
    }

    public Instant getLastSuccess() {
        return lastSuccess;
    }

    public void setLastSuccess(Instant lastSuccess) {
        this.lastSuccess = lastSuccess;
    }

    public Instant getLastFailure() {
        return lastFailure;
    }

    public void setLastFailure(Instant lastFailure) {
        this.lastFailure = lastFailure;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }

    public void setConsecutiveFailures(int consecutiveFailures) {
        this.consecutiveFailures = consecutiveFailures;
    }

    public Instant getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(Instant lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TaskEntity that = (TaskEntity) o;
        return picked == that.picked && pickedBy == that.pickedBy
            && consecutiveFailures == that.consecutiveFailures && version == that.version
            && Objects.equals(taskName, that.taskName) && Objects
            .equals(taskInstance, that.taskInstance) && Arrays.equals(taskData, that.taskData)
            && Objects.equals(executionTime, that.executionTime) && Objects
            .equals(lastSuccess, that.lastSuccess) && Objects
            .equals(lastFailure, that.lastFailure) && Objects
            .equals(lastHeartbeat, that.lastHeartbeat);
    }

    @Override
    public int hashCode() {
        int result = Objects
            .hash(super.hashCode(), taskName, taskInstance, executionTime, picked, pickedBy,
                lastSuccess,
                lastFailure, consecutiveFailures, lastHeartbeat, version);
        result = 31 * result + Arrays.hashCode(taskData);
        return result;
    }

    @Override
    public String toString() {
        return "TaskEntity{" +
            "taskName='" + taskName + '\'' +
            ", taskInstance='" + taskInstance + '\'' +
            ", taskData=" + Arrays.toString(taskData) +
            ", executionTime=" + executionTime +
            ", picked=" + picked +
            ", pickedBy=" + pickedBy +
            ", lastSuccess=" + lastSuccess +
            ", lastFailure=" + lastFailure +
            ", consecutiveFailures=" + consecutiveFailures +
            ", lastHeartbeat=" + lastHeartbeat +
            ", version=" + version +
            '}';
    }


}
