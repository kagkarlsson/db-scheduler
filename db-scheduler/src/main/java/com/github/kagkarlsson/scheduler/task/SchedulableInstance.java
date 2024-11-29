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

import java.time.Instant;
import java.util.function.Supplier;

public interface SchedulableInstance<T> extends TaskInstanceId {

  static <T> SchedulableInstance<T> of(TaskInstance<T> taskInstance, Instant executionTime) {
    return new SchedulableTaskInstance<T>(taskInstance, executionTime);
  }

  static <T> SchedulableInstance<T> of(
      TaskInstance<T> taskInstance, NextExecutionTime executionTime) {
    return new SchedulableTaskInstance<T>(taskInstance, executionTime);
  }

  TaskInstance<T> getTaskInstance();

  Instant getNextExecutionTime(Instant currentTime);

  default String getTaskName() {
    return getTaskInstance().getTaskName();
  }

  default String getId() {
    return getTaskInstance().getId();
  }

  class Builder<T> {

    private final String taskName;
    private final String id;
    private Supplier<T> dataSupplier = () -> (T) null;
    private int priority = Priority.MEDIUM;

    public Builder(String taskName, String id) {
      this.id = id;
      this.taskName = taskName;
    }

    public SchedulableInstance.Builder<T> data(Supplier<T> dataSupplier) {
      this.dataSupplier = dataSupplier;
      return this;
    }

    public SchedulableInstance.Builder<T> data(T data) {
      this.dataSupplier = () -> (T) data;
      return this;
    }

    public SchedulableInstance.Builder<T> priority(int priority) {
      this.priority = priority;
      return this;
    }

    public SchedulableInstance<T> scheduledTo(Instant executionTime) {
      TaskInstance<T> taskInstance = new TaskInstance<>(taskName, id, dataSupplier, priority);
      return new SchedulableTaskInstance<>(taskInstance, executionTime);
    }
  }
}
