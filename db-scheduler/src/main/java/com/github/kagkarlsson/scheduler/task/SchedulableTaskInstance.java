/**
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

public class SchedulableTaskInstance<T> implements SchedulableInstance<T> {
  private final TaskInstance<T> taskInstance;
  NextExecutionTime executionTime;

  public SchedulableTaskInstance(TaskInstance<T> taskInstance, NextExecutionTime executionTime) {
    this.taskInstance = taskInstance;
    this.executionTime = executionTime;
  }

  public SchedulableTaskInstance(TaskInstance<T> taskInstance, Instant executionTime) {
    this.taskInstance = taskInstance;
    this.executionTime = (_ignored) -> executionTime;
  }

  @Override
  public TaskInstance<T> getTaskInstance() {
    return taskInstance;
  }

  @Override
  public Instant getNextExecutionTime(Instant currentTime) {
    return executionTime.getNextExecutionTime(currentTime);
  }
}
