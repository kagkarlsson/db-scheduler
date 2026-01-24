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

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public class ExecutionComplete {
  private final Execution execution;
  private final Instant timeStarted;
  private final Instant timeDone;
  private final Result result;
  private final Throwable cause;

  ExecutionComplete(
      Execution execution, Instant timeStarted, Instant timeDone, Result result, Throwable cause) {
    this.timeStarted = timeStarted;
    this.cause = cause;
    if (result == Result.OK && cause != null) {
      throw new IllegalArgumentException("Result 'OK' should never have a cause.");
    }
    this.execution = execution;
    this.timeDone = timeDone;
    this.result = result;
  }

  public static ExecutionComplete success(
      Execution execution, Instant timeStarted, Instant timeDone) {
    return new ExecutionComplete(execution, timeStarted, timeDone, Result.OK, null);
  }

  public static ExecutionComplete failure(
      Execution execution, Instant timeStarted, Instant timeDone, Throwable cause) {
    return new ExecutionComplete(execution, timeStarted, timeDone, Result.FAILED, cause);
  }

  /** Simulated ExecutionComplete used to generate first execution-time from a Schedule. */
  public static ExecutionComplete simulatedSuccess(Instant timeDone) {
    TaskInstance nonExistingTaskInstance = new TaskInstance("non-existing-task", "non-existing-id");
    Execution nonExistingExecution =
        new Execution(
            timeDone,
            nonExistingTaskInstance,
            false,
            "simulated-picked-by",
            timeDone,
            null,
            0,
            null,
            1,
            null);
    return new ExecutionComplete(
        nonExistingExecution, timeDone.minus(Duration.ofSeconds(1)), timeDone, Result.OK, null);
  }

  public Execution getExecution() {
    return execution;
  }

  public Instant getTimeDone() {
    return timeDone;
  }

  public Duration getDuration() {
    return Duration.between(timeStarted, timeDone);
  }

  public Result getResult() {
    return result;
  }

  public Optional<Throwable> getCause() {
    return Optional.ofNullable(cause);
  }

  public enum Result {
    OK,
    FAILED
  }
}
