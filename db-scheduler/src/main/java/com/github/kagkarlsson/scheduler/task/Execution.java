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

import com.github.kagkarlsson.scheduler.Resolvable;
import java.time.Instant;
import java.util.Objects;

@SuppressWarnings("rawtypes")
public final class Execution implements TaskInstanceId, Resolvable {
  public final TaskInstance taskInstance;
  public final Instant executionTime;
  public final boolean picked;
  public final String pickedBy;
  public int consecutiveFailures;
  public final Instant lastHeartbeat;
  public final long version;
  public final Instant lastFailure;
  public final Instant lastSuccess;
  public final State state;

  public Execution(Instant executionTime, TaskInstance taskInstance) {
    this(executionTime, taskInstance, false, null, null, null, 0, null, 1L, null);
  }

  public Execution(
      Instant executionTime,
      TaskInstance taskInstance,
      boolean picked,
      String pickedBy,
      Instant lastSuccess,
      Instant lastFailure,
      int consecutiveFailures,
      Instant lastHeartbeat,
      long version,
      State state) {
    this.executionTime = executionTime;
    this.taskInstance = taskInstance;
    this.picked = picked;
    this.pickedBy = pickedBy;
    this.lastFailure = lastFailure;
    this.lastSuccess = lastSuccess;
    this.consecutiveFailures = consecutiveFailures;
    this.lastHeartbeat = lastHeartbeat;
    this.version = version;
    this.state = state;
  }

  @Override
  public Instant getExecutionTime() {
    return executionTime;
  }

  public boolean isPicked() {
    return picked;
  }

  public Execution updateToPicked(String newPickedBy, Instant newLastHeartbeat) {
    return new Execution(
        executionTime,
        taskInstance,
        true,
        newPickedBy,
        lastSuccess,
        lastFailure,
        consecutiveFailures,
        newLastHeartbeat,
        version + 1, // since this was incremented in the database when picked
        state);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Execution execution = (Execution) o;
    return Objects.equals(executionTime, execution.executionTime)
        && Objects.equals(taskInstance, execution.taskInstance);
  }

  @Override
  public int hashCode() {
    return Objects.hash(executionTime, taskInstance);
  }

  @Override
  public String toString() {
    return "Execution: "
        + "task="
        + taskInstance.getTaskName()
        + ", id="
        + taskInstance.getId()
        + ", executionTime="
        + executionTime
        + ", picked="
        + picked
        + ", pickedBy="
        + pickedBy
        + ", lastHeartbeat="
        + lastHeartbeat
        + ", version="
        + version;
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
