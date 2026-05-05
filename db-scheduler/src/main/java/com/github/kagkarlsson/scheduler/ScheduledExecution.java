/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.exceptions.DataClassMismatchException;
import com.github.kagkarlsson.scheduler.exceptions.MissingRawDataException;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.Objects;

@SuppressWarnings("rawtypes")
public record ScheduledExecution<DATA_TYPE>(
    Class<DATA_TYPE> dataClass,
    TaskInstanceId taskInstance,
    Instant executionTime,
    Instant lastSuccess,
    Instant lastFailure,
    int consecutiveFailures,
    boolean picked,
    String pickedBy,
    Object taskData) {

  static <T> ScheduledExecution<T> from(Class<T> dataClass, Execution execution) {
    return new ScheduledExecution<>(
        dataClass,
        execution.taskInstance,
        execution.executionTime,
        execution.lastSuccess,
        execution.lastFailure,
        execution.consecutiveFailures,
        execution.picked,
        execution.pickedBy,
        execution.taskInstance.getData());
  }

  public TaskInstanceId getTaskInstance() {
    return taskInstance;
  }

  public Instant getExecutionTime() {
    return executionTime;
  }

  @SuppressWarnings("unchecked")
  public DATA_TYPE getData() {
    if (taskData == null) {
      return null;
    } else if (dataClass.isInstance(taskData)) {
      return (DATA_TYPE) taskData;
    }
    throw new DataClassMismatchException(dataClass, taskData.getClass());
  }

  public boolean hasRawData() {
    return taskData == null || taskData.getClass().equals(byte[].class);
  }

  public byte[] getRawData() {
    if (!hasRawData()) {
      throw new MissingRawDataException(dataClass);
    }
    return (byte[]) taskData;
  }

  public Instant getLastSuccess() {
    return lastSuccess;
  }

  public Instant getLastFailure() {
    return lastFailure;
  }

  public int getConsecutiveFailures() {
    return consecutiveFailures;
  }

  public boolean isPicked() {
    return picked;
  }

  public String getPickedBy() {
    return pickedBy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ScheduledExecution<?> that = (ScheduledExecution<?>) o;
    return Objects.equals(executionTime, that.executionTime)
        && Objects.equals(taskInstance, that.taskInstance);
  }

  @Override
  public int hashCode() {
    return Objects.hash(executionTime, taskInstance);
  }
}
