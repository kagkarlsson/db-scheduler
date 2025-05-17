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

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SchedulerState {

  boolean isShuttingDown();

  boolean isStarted();

  boolean isPaused();

  class SettableSchedulerState implements SchedulerState {

    private static final Logger LOG = LoggerFactory.getLogger(SettableSchedulerState.class);
    private boolean isShuttingDown;
    private boolean isStarted;
    private final AtomicBoolean isPaused = new AtomicBoolean(false);

    @Override
    public boolean isShuttingDown() {
      return isShuttingDown;
    }

    @Override
    public boolean isStarted() {
      return isStarted;
    }

    @Override
    public boolean isPaused() {
      return isPaused.get();
    }

    public void setIsShuttingDown() {
      this.isShuttingDown = true;
    }

    public void setStarted() {
      this.isStarted = true;
    }

    public void setPaused(boolean isPaused) {
      boolean isChanged = this.isPaused.compareAndSet(!isPaused, isPaused);
      if (isChanged) {
        LOG.info(isPaused ? "Scheduler is paused." : "Scheduler is resumed.");
      }
    }
  }
}
