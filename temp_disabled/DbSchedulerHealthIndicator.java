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
package com.github.kagkarlsson.scheduler.boot.actuator;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerState;
import java.util.Objects;
import org.springframework.boot.actuator.health.Health;
import org.springframework.boot.actuator.health.HealthIndicator;

public class DbSchedulerHealthIndicator implements HealthIndicator {
  private final SchedulerState state;

  public DbSchedulerHealthIndicator(Scheduler scheduler) {
    this.state = Objects.requireNonNull(scheduler).getSchedulerState();
  }

  @Override
  public Health health() {
    if (state.isStarted() && !state.isShuttingDown()) {
      return Health.up().withDetail("state", "started").build();
    } else if (state.isStarted() && state.isShuttingDown()) {
      return Health.outOfService().withDetail("state", "shutting_down").build();
    } else if (!state.isStarted() && !state.isShuttingDown()) {
      return Health.down().withDetail("state", "not_started").build();
    } else {
      return Health.down().build();
    }
  }
}
