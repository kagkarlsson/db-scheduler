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
package com.github.kagkarlsson.scheduler.boot.config.shutdown;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerState;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerStopper;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSchedulerStopper implements DbSchedulerStopper {
  private final Logger log = LoggerFactory.getLogger(this.getClass());
  private final Scheduler scheduler;

  protected AbstractSchedulerStopper(Scheduler scheduler) {
    this.scheduler = Objects.requireNonNull(scheduler, "A scheduler must be provided");
  }

  @Override
  public void doStop() {
    SchedulerState state = scheduler.getSchedulerState();

    if (state.isShuttingDown()) {
      log.warn("Scheduler is already shutting down - will not attempt to stop");
      return;
    }

    if (!state.isStarted()) {
      log.warn("Scheduler not started - will not attempt to stop");
      return;
    }

    log.info("Triggering scheduler stop");
    scheduler.stop();
  }
}
