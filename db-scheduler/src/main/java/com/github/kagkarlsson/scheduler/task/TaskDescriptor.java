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

public interface TaskDescriptor<T> extends HasTaskName {

  String getTaskName();

  Class<T> getDataClass();

  static TaskDescriptor<Void> of(String name) {
    return TaskDescriptor.of(name, Void.class);
  }

  static <T> TaskDescriptor<T> of(String name, Class<T> dataClass) {
    return new TaskDescriptor.SimpleTaskDescriptor<>(name, dataClass);
  }

  default TaskInstance.Builder<T> instance(String id) {
    return new TaskInstance.Builder<>(getTaskName(), id);
  }

  default TaskInstanceId instanceId(String id) {
    return TaskInstanceId.of(getTaskName(), id);
  }

  class SimpleTaskDescriptor<T> implements TaskDescriptor<T> {

    private final String name;
    private final Class<T> dataClass;

    public SimpleTaskDescriptor(String name, Class<T> dataClass) {
      this.name = name;
      this.dataClass = dataClass;
    }

    @Override
    public String getTaskName() {
      return name;
    }

    @Override
    public Class<T> getDataClass() {
      return dataClass;
    }
  }
}
