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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Waiter {

  private static final Logger LOG = LoggerFactory.getLogger(Waiter.class);

  private final ReentrantLock lock;
  private final Condition condition;
  private boolean woken = false;
  private final Duration duration;
  private final Clock clock;
  private boolean isWaiting = false;
  private boolean skipNextWait = false;

  public Waiter(Duration duration) {
    this(duration, new SystemClock());
  }

  public Waiter(Duration duration, Clock clock) {
    this.duration = duration;
    this.clock = clock;
    this.lock = new ReentrantLock();
    this.condition = this.lock.newCondition();
  }

  Waiter(Duration duration, Clock clock, ReentrantLock lock, Condition condition) {
    this.duration = duration;
    this.clock = clock;
    this.lock = lock;
    this.condition = condition;
  }

  public void doWait() throws InterruptedException {
    long millis = duration.toMillis();

    if (millis <= 0) {
      return;
    }

    Instant waitUntil = clock.now().plusMillis(millis);

    while (clock.now().isBefore(waitUntil)) {
      lock.lock();
      try {
        if (skipNextWait) {
          LOG.debug("Waiter has been notified to skip next wait-period. Skipping wait.");
          skipNextWait = false;
          return;
        }

        woken = false;
        LOG.debug("Waiter start wait.");
        this.isWaiting = true;
        condition.awaitNanos(millis * 1_000_000);
        this.isWaiting = false;
        if (woken) {
          LOG.debug(
            "Waiter woken, it had {}ms left to wait.",
            (waitUntil.toEpochMilli() - clock.now().toEpochMilli()));
          return;
        }

      } finally {
        lock.unlock();
      }
    }
  }

  boolean wake() {
    return withLock(lock, () -> {
      if (!isWaiting) {
        return false;
      } else {
        woken = true;
        condition.signal();
        return true;
      }
    });
  }

  public void wakeOrSkipNextWait() {
    // Take early lock to avoid race-conditions. Lock is also taken in wake() (lock is re-entrant)
    withLock(lock, () -> {
      final boolean awoken = wake();
      if (!awoken) {
        LOG.debug("Waiter not waiting, instructing to skip next wait.");
        this.skipNextWait = true;
      }
      return null;
    });
  }

  public Duration getWaitDuration() {
    return duration;
  }

  public boolean isWaiting() {
    return isWaiting;
  }

  public static <T> T withLock(ReentrantLock lock, Supplier<T> action) {
    lock.lock();
    try {
      return action.get();
    } finally {
      lock.unlock();
    }
  }

  public static <T> T withLock(ReentrantLock lock, Runnable action) {
    return withLock(lock, () -> {
      action.run();
      return null;
    });
  }
}
