/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;

class RunUntilShutdown implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RunUntilShutdown.class);
    private Runnable toRun;
    private Callable<OverriddenWaitDuration> toRunCallable;
    private final Waiter waitBetweenRuns;
    private final SchedulerState schedulerState;
    private final StatsRegistry statsRegistry;

    public RunUntilShutdown(Runnable toRun, Waiter waitBetweenRuns, SchedulerState schedulerState, StatsRegistry statsRegistry) {
        this(waitBetweenRuns, schedulerState, statsRegistry);
        this.toRun = toRun;
    }

    public RunUntilShutdown(Callable<OverriddenWaitDuration> toRun, Waiter waitBetweenRuns, SchedulerState schedulerState, StatsRegistry statsRegistry) {
        this(waitBetweenRuns, schedulerState, statsRegistry);
        this.toRunCallable = toRun;
    }

    private RunUntilShutdown(Waiter waitBetweenRuns, SchedulerState schedulerState, StatsRegistry statsRegistry) {
        this.waitBetweenRuns = waitBetweenRuns;
        this.schedulerState = schedulerState;
        this.statsRegistry = statsRegistry;
    }

    @Override
    public void run() {
        while (!schedulerState.isShuttingDown()) {
            OverriddenWaitDuration overriddenWait = OverriddenWaitDuration.absent();
            try {
                if (toRunCallable != null) {
                    overriddenWait = toRunCallable.call();
                } else {
                    toRun.run();
                }
            } catch (Throwable e) {
                LOG.error("Unhandled exception. Will keep running.", e);
                statsRegistry.register(StatsRegistry.SchedulerStatsEvent.UNEXPECTED_ERROR);
            }

            try {
                if (overriddenWait.isSet()) {
                    waitBetweenRuns.doWait(overriddenWait.get());
                } else {
                    waitBetweenRuns.doWait();
                }
            } catch (InterruptedException interruptedException) {
                if (schedulerState.isShuttingDown()) {
                    LOG.debug("Thread '{}' interrupted due to shutdown.", Thread.currentThread().getName());
                } else {
                    LOG.error("Unexpected interruption of thread. Will keep running.", interruptedException);
                    statsRegistry.register(StatsRegistry.SchedulerStatsEvent.UNEXPECTED_ERROR);
                }
            }
        }
    }

    public static class OverriddenWaitDuration {
        private final Duration duration;

        public OverriddenWaitDuration(Duration duration) {
            this.duration = duration;
        }

        public static OverriddenWaitDuration of(Duration duration) {
            return new OverriddenWaitDuration(duration);
        }

        public static OverriddenWaitDuration absent() {
            return new OverriddenWaitDuration(null);
        }

        public boolean isSet() {
            return duration != null;
        }

        public Duration get() {
            return duration;
        }
    }
}
