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
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.time.Duration;
import java.time.Instant;

@SuppressWarnings("rawtypes")
public class CurrentlyExecuting {

  private final Execution execution;
  private final Clock clock;
  private final Instant startTime;

  public CurrentlyExecuting(Execution execution, Clock clock) {
    this.execution = execution;
    this.clock = clock;
    this.startTime = clock.now();
  }

  public Duration getDuration() {
    return Duration.between(startTime, clock.now());
  }

  public TaskInstance getTaskInstance() {
    return execution.taskInstance;
  }

  public Execution getExecution() {
    return execution;
  }
}
