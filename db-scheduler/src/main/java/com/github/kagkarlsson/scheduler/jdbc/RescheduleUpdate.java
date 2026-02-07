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
package com.github.kagkarlsson.scheduler.jdbc;

import com.github.kagkarlsson.scheduler.jdbc.ExecutionUpdate.NewValue;
import java.time.Instant;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

@NullMarked
public record RescheduleUpdate(
    Instant executionTime,
    @Nullable NewValue<Object> data,
    @Nullable NewValue<Instant> lastSuccess,
    @Nullable NewValue<Instant> lastFailure,
    @Nullable NewValue<Integer> consecutiveFailures) {

  public static Builder to(Instant executionTime) {
    return new Builder(executionTime);
  }

  public static class Builder {
    private final Instant executionTime;
    @Nullable private NewValue<Object> data;
    @Nullable private NewValue<Instant> lastSuccess;
    @Nullable private NewValue<Instant> lastFailure;
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

    public Builder consecutiveFailures(int count) {
      this.consecutiveFailures = NewValue.of(count);
      return this;
    }

    /** Convenience method for resetting to not-failing */
    public Builder resetFailures() {
      this.consecutiveFailures = NewValue.of(0);
      this.lastFailure = new NewValue<>(null);
      return this;
    }

    public RescheduleUpdate build() {
      return new RescheduleUpdate(
          executionTime, data, lastSuccess, lastFailure, consecutiveFailures);
    }
  }
}
