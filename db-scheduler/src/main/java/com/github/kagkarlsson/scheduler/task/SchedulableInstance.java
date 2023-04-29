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

public interface SchedulableInstance<T> extends TaskInstanceId {

  TaskInstance<T> getTaskInstance();

  Instant getNextExecutionTime(Instant currentTime);

  default String getTaskName() {
    return getTaskInstance().getTaskName();
  }

  default String getId() {
    return getTaskInstance().getId();
  }

  static <T> SchedulableInstance<T> of(TaskInstance<T> taskInstance, Instant executionTime) {
    return new SchedulableTaskInstance<T>(taskInstance, executionTime);
  }

  static <T> SchedulableInstance<T> of(
      TaskInstance<T> taskInstance, NextExecutionTime executionTime) {
    return new SchedulableTaskInstance<T>(taskInstance, executionTime);
  }
}
