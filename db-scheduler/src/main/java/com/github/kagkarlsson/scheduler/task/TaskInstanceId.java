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

import java.util.Objects;

public interface TaskInstanceId extends HasTaskName {
  @Override
  String getTaskName();

  String getId();

  static TaskInstanceId of(String taskName, String id) {
    return new StandardTaskInstanceId(taskName, id);
  }

  class StandardTaskInstanceId implements TaskInstanceId {
    private final String taskName;
    private final String id;

    public StandardTaskInstanceId(String taskName, String id) {
      this.taskName = taskName;
      this.id = id;
    }

    @Override
    public String getTaskName() {
      return this.taskName;
    }

    @Override
    public String getId() {
      return this.id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StandardTaskInstanceId that = (StandardTaskInstanceId) o;
      return Objects.equals(taskName, that.taskName) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(taskName, id);
    }
  }
}
