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
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

@NullMarked
public record RescheduleUpdate(
    Instant executionTime,
    @Nullable NewValue<Object> data,
    @Nullable NewValue<Instant> lastSuccess,
    @Nullable NewValue<Instant> lastFailure,
    @Nullable NewValue<String> lastFailureText,
    @Nullable NewValue<Integer> consecutiveFailures) {

  public static Builder toExecutionTime(Instant executionTime) {
    return new Builder(executionTime);
  }

  /** Sets {@code lastFailureText} to the given value, unless the update already sets it. */
  RescheduleUpdate withLastFailureTextIfUnset(@Nullable String text) {
    if (lastFailureText != null) {
      return this;
    }
    return new RescheduleUpdate(
        executionTime, data, lastSuccess, lastFailure, new NewValue<>(text), consecutiveFailures);
  }

  public static class Builder {
    private final Instant executionTime;
    @Nullable private NewValue<Object> data;
    @Nullable private NewValue<Instant> lastSuccess;
    @Nullable private NewValue<Instant> lastFailure;
    @Nullable private NewValue<String> lastFailureText;
    @Nullable private NewValue<Integer> consecutiveFailures;

    public Builder(Instant executionTime) {
      this.executionTime = executionTime;
    }

    public Builder data(@Nullable Object data) {
      this.data = new NewValue<>(data);
      return this;
    }

    public Builder lastSuccess(@Nullable Instant lastSuccess) {
      this.lastSuccess = new NewValue<>(lastSuccess);
      return this;
    }

    public Builder lastFailure(@Nullable Instant lastFailure) {
      this.lastFailure = new NewValue<>(lastFailure);
      return this;
    }

    public Builder lastFailureText(@Nullable String lastFailureText) {
      this.lastFailureText = new NewValue<>(lastFailureText);
      return this;
    }

    public Builder consecutiveFailures(int count) {
      this.consecutiveFailures = new NewValue<>(count);
      return this;
    }

    /** Convenience method for resetting to not-failing */
    public Builder resetFailures() {
      this.consecutiveFailures = new NewValue<>(0);
      this.lastFailure = new NewValue<>(null);
      this.lastFailureText = new NewValue<>(null);
      return this;
    }

    public RescheduleUpdate build() {
      return new RescheduleUpdate(
          executionTime, data, lastSuccess, lastFailure, lastFailureText, consecutiveFailures);
    }
  }
}
