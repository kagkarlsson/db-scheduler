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

public interface Task<T> extends ExecutionHandler<T>, HasTaskName {

  String getName();

  Class<T> getDataClass();

  TaskInstance<T> instance(String id);

  TaskInstance<T> instance(String id, T data);

  TaskInstance.Builder<T> instanceBuilder(String id);

  default TaskInstanceId instanceId(String id) {
    return TaskInstanceId.of(getName(), id);
  }

  SchedulableInstance<T> schedulableInstance(String id);

  SchedulableInstance<T> schedulableInstance(String id, T data);

  FailureHandler<T> getFailureHandler();

  DeadExecutionHandler<T> getDeadExecutionHandler();

  @Override
  default String getTaskName() {
    return getName();
  }

  default int getDefaultPriority() {
    return Priority.MEDIUM;
  }
}
