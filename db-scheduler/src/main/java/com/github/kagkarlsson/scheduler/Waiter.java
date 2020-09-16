/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public class Waiter {
    private static final Logger LOG = LoggerFactory.getLogger(Waiter.class);

    private Object lock;
    private boolean woken = false;
    private final Duration duration;
    private Clock clock;
    private boolean isWaiting = false;
    private boolean skipNextWait = false;

    public Waiter(Duration duration) {
        this(duration, new SystemClock());
    }

    public Waiter(Duration duration, Clock clock) {
        this(duration, clock, new Object());
    }

    Waiter(Duration duration, Clock clock, Object lock) {
        this.duration = duration;
        this.clock = clock;
        this.lock = lock;
    }

    public void doWait() throws InterruptedException {
        if (skipNextWait) {
            LOG.debug("Waiter has been notified to skip next wait-period. Skipping wait.");
            skipNextWait = false;
            return;
        }

        long millis = duration.toMillis();

        if (millis > 0) {
            Instant waitUntil = clock.now().plusMillis(millis);

            while (clock.now().isBefore(waitUntil)) {
                synchronized (lock) {
                    woken = false;
                    LOG.debug("Waiter start wait.");
                    this.isWaiting = true;
                    lock.wait(millis);
                    this.isWaiting = false;
                    if (woken) {
                        LOG.debug("Waiter woken, it had {}ms left to wait.", (waitUntil.toEpochMilli() - clock.now().toEpochMilli()));
                        break;
                    }
                }
            }
        }
    }

    public boolean wake() {
        synchronized (lock) {
            if (!isWaiting) {
                return false;
            } else {
                woken = true;
                lock.notify();
                return true;
            }
        }
    }

    public void wakeOrSkipNextWait() {
        final boolean awoken = wake();
        if (!awoken) {
            this.skipNextWait = true;
        }
    }

    public Duration getWaitDuration() {
        return duration;
    }

}
