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

import com.github.kagkarlsson.scheduler.Clock;
import java.time.Instant;

public class ScheduledTaskInstance implements TaskInstanceId {
  private final TaskInstance<?> taskInstance;
  private final Instant executionTime;

  public ScheduledTaskInstance(TaskInstance<?> taskInstance, Instant executionTime) {
    this.taskInstance = taskInstance;
    this.executionTime = executionTime;
  }

  public static ScheduledTaskInstance fixExecutionTime(
      SchedulableInstance<?> schedulableInstance, Clock clock) {
    return new ScheduledTaskInstance(
        schedulableInstance.getTaskInstance(),
        schedulableInstance.getNextExecutionTime(clock.now()));
  }

  public TaskInstance<?> getTaskInstance() {
    return taskInstance;
  }

  public Instant getExecutionTime() {
    return executionTime;
  }

  @Override
  public String getTaskName() {
    return taskInstance.getTaskName();
  }

  @Override
  public String getId() {
    return taskInstance.getId();
  }
}
