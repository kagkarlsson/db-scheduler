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
import com.github.kagkarlsson.scheduler.task.State;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

public record DeactivationUpdate(
    @Nullable NewValue<Instant> lastSuccess,
    @Nullable NewValue<Instant> lastFailed,
    @Nullable NewValue<State> state) {

  public static Builder toState(State state) {
    return new Builder(state);
  }

  public static class Builder {
    @Nullable private NewValue<Instant> lastSuccess;
    @Nullable private NewValue<Instant> lastFailed;
    @Nullable private NewValue<State> state;

    public Builder(State state) {
      this.state = NewValue.of(state);
    }

    public Builder lastSuccess(Instant lastSuccess) {
      this.lastSuccess = NewValue.of(lastSuccess);
      return this;
    }

    public Builder lastFailed(Instant lastFailed) {
      this.lastFailed = NewValue.of(lastFailed);
      return this;
    }

    public Builder state(State state) {
      this.state = NewValue.of(state);
      return this;
    }

    public DeactivationUpdate build() {
      return new DeactivationUpdate(lastSuccess, lastFailed, state);
    }
  }
}
