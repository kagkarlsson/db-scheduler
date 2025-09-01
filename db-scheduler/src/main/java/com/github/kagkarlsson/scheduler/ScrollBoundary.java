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

import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

public class ScrollBoundary {
  private final Instant executionTime;
  private final String taskInstanceId;

  public ScrollBoundary(Instant executionTime, String taskInstanceId) {
    this.executionTime = Objects.requireNonNull(executionTime, "Execution time cannot be null");
    this.taskInstanceId = Objects.requireNonNull(taskInstanceId, "Task instance ID cannot be null");
  }

  public static ScrollBoundary from(ScheduledExecution<?> execution) {
    return new ScrollBoundary(execution.getExecutionTime(), execution.getTaskInstance().getId());
  }

  public Instant getExecutionTime() {
    return executionTime;
  }

  public String getTaskInstanceId() {
    return taskInstanceId;
  }

  public String toEncodedPointOfScroll() {
    String combined = executionTime.toEpochMilli() + ":" + taskInstanceId;
    return Base64.getEncoder().encodeToString(combined.getBytes());
  }

  public static ScrollBoundary fromEncodedPoint(String pointOfScroll) {
    try {
      String decoded = new String(Base64.getDecoder().decode(pointOfScroll));
      String[] parts = decoded.split(":", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid boundary format");
      }

      return new ScrollBoundary(Instant.ofEpochMilli(Long.parseLong(parts[0])), parts[1]);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid boundary: " + pointOfScroll, e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ScrollBoundary that = (ScrollBoundary) o;
    return Objects.equals(executionTime, that.executionTime)
        && Objects.equals(taskInstanceId, that.taskInstanceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(executionTime, taskInstanceId);
  }

  @Override
  public String toString() {
    return "ScrollBoundary{"
        + "executionTime="
        + executionTime
        + ", taskInstanceId='"
        + taskInstanceId
        + '\''
        + '}';
  }
}
