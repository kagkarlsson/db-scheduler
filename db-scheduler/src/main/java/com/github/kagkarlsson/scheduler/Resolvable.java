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

import com.github.kagkarlsson.scheduler.task.HasTaskName;
import java.time.Instant;

public interface Resolvable extends HasTaskName {

  Instant getExecutionTime();

  static Resolvable of(String taskName, Instant executionTime) {
    return new SimpleResolvable(taskName, executionTime);
  }

  record SimpleResolvable(String taskName, Instant executionTime) implements Resolvable {

    @Override
    public Instant getExecutionTime() {
      return executionTime;
    }

    @Override
    public String getTaskName() {
      return taskName;
    }
  }
}
