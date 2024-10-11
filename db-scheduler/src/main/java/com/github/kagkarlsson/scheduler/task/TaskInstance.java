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
package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

public class TaskInstance<T> implements TaskInstanceId {

  private final String taskName;
  private final String id;
  private final Supplier<T> dataSupplier;
  private final int priority;

  public TaskInstance(String taskName, String id) {
    this(taskName, id, (T) null);
  }

  public TaskInstance(String taskName, String id, T data) {
    this(taskName, id, () -> data, Priority.MEDIUM);
  }

  public TaskInstance(String taskName, String id, Supplier<T> dataSupplier, int priority) {
    this.taskName = taskName;
    this.id = id;
    this.dataSupplier = dataSupplier;
    this.priority = priority;
  }

  public String getTaskAndInstance() {
    return taskName + "_" + id;
  }

  public String getTaskName() {
    return taskName;
  }

  @Override
  public String getId() {
    return id;
  }

  public T getData() {
    return dataSupplier.get();
  }

  public int getPriority() {
    return priority;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskInstance<?> that = (TaskInstance<?>) o;
    return priority == that.priority
        && Objects.equals(taskName, that.taskName)
        && Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskName, id, priority);
  }

  @Override
  public String toString() {
    return "TaskInstance: " + "task=" + taskName + ", id=" + id + ", priority=" + priority;
  }

  public static class Builder<T> {

    private final String taskName;
    private final String id;
    private Supplier<T> dataSupplier = () -> (T) null;
    private int priority = Priority.MEDIUM;

    public Builder(String taskName, String id) {
      this.id = id;
      this.taskName = taskName;
    }

    public Builder<T> dataSupplier(Supplier<T> dataSupplier) {
      this.dataSupplier = dataSupplier;
      return this;
    }

    public Builder<T> data(T data) {
      this.dataSupplier = () -> (T) data;
      ;
      return this;
    }

    public Builder<T> priority(int priority) {
      this.priority = priority;
      return this;
    }

    public TaskInstance<T> build() {
      return new TaskInstance<>(taskName, id, dataSupplier, priority);
    }

    public SchedulableInstance<T> scheduledTo(Instant executionTime) {
      TaskInstance<T> taskInstance = new TaskInstance<>(taskName, id, dataSupplier, priority);
      return new SchedulableTaskInstance<>(taskInstance, executionTime);
    }

    public SchedulableInstance<T> scheduledAccordingToData() {
      TaskInstance<T> taskInstance = new TaskInstance<>(taskName, id, dataSupplier, priority);
      T data = dataSupplier.get();
      if (!(data instanceof ScheduleAndData)) {
        throw new RuntimeException(
            "To be able to use method 'scheduledAccordingToData()', dataClass must implement ScheduleAndData interface and contain a Schedule");
      }

      ScheduleAndData scheduleAndData = (ScheduleAndData) data;

      return new SchedulableTaskInstance<>(
          taskInstance, scheduleAndData.getSchedule()::getInitialExecutionTime);
    }
  }
}
