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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record ExecutionTimeAndId(Instant executionTime, String taskInstanceId) {

  public ExecutionTimeAndId(Instant executionTime, String taskInstanceId) {
    this.executionTime = Objects.requireNonNull(executionTime, "Execution time cannot be null");
    this.taskInstanceId = Objects.requireNonNull(taskInstanceId, "Task instance ID cannot be null");
  }

  public static ExecutionTimeAndId from(ScheduledExecution<?> execution) {
    return new ExecutionTimeAndId(
        execution.getExecutionTime(), execution.getTaskInstance().getId());
  }

  private static final Pattern PATTERN =
      Pattern.compile("executionTime\\((\\d+),(\\d+)\\)taskInstanceId\\((.*)\\)");

  public String toEncodedString() {
    String template =
        "executionTime("
            + executionTime.getEpochSecond()
            + ","
            + executionTime.getNano()
            + ")taskInstanceId("
            + taskInstanceId
            + ")";
    return Base64.getEncoder().encodeToString(template.getBytes());
  }

  public static ExecutionTimeAndId fromEncodedString(String encoded) {
    try {
      String decoded = new String(Base64.getDecoder().decode(encoded));
      Matcher matcher = PATTERN.matcher(decoded);

      if (!matcher.matches()) {
        throw new IllegalArgumentException("Invalid execution time and id format");
      }

      long epochSecond = Long.parseLong(matcher.group(1));
      int nanos = Integer.parseInt(matcher.group(2));
      String taskId = matcher.group(3);

      return new ExecutionTimeAndId(Instant.ofEpochSecond(epochSecond, nanos), taskId);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid execution time and id: " + encoded, e);
    }
  }
}
