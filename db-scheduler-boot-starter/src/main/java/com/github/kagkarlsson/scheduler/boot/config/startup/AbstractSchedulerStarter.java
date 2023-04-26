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
package com.github.kagkarlsson.scheduler.boot.config.startup;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerState;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerStarter;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSchedulerStarter implements DbSchedulerStarter {
  private final Logger log = LoggerFactory.getLogger(this.getClass());
  private final Scheduler scheduler;

  protected AbstractSchedulerStarter(Scheduler scheduler) {
    this.scheduler = Objects.requireNonNull(scheduler, "A scheduler must be provided");
  }

  @Override
  public void doStart() {
    SchedulerState state = scheduler.getSchedulerState();

    if (state.isShuttingDown()) {
      log.warn("Scheduler is shutting down - will not attempting to start");
      return;
    }

    if (state.isStarted()) {
      log.info("Scheduler already started - will not attempt to start again");
      return;
    }

    log.info("Triggering scheduler start");
    scheduler.start();
  }
}
