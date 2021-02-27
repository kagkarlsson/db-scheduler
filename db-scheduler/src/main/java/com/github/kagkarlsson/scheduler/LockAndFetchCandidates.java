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

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class LockAndFetchCandidates implements PollStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(LockAndFetchCandidates.class);
    private final Scheduler scheduler;
    private final PollingStrategyConfig pollingStrategyConfig;
    private final int lowerLimit;
    private final int upperLimit;
    private AtomicBoolean moreExecutionsInDatabase = new AtomicBoolean(false);

    public LockAndFetchCandidates(Scheduler scheduler, PollingStrategyConfig pollingStrategyConfig) {
        this.scheduler = scheduler;
        this.pollingStrategyConfig = pollingStrategyConfig;
        lowerLimit = pollingStrategyConfig.getLowerLimit(scheduler.threadpoolSize);
        upperLimit = pollingStrategyConfig.getUpperLimit(scheduler.threadpoolSize);
    }

    @Override
    public void run() {
        Instant now = scheduler.clock.now();

        int executionsToFetch = upperLimit - scheduler.executor.getNumberInQueueOrProcessing();

        // Might happen if upperLimit == threads and all threads are busy
        if (executionsToFetch <= 0) {
            LOG.trace("No executions to fetch.");
            return;
        }

        // FIXLATER: should it fetch here if not under lowerLimit? probably
        List<Execution> pickedExecutions = scheduler.schedulerTaskRepository.lockAndGetDue(now, executionsToFetch);
        LOG.trace("Picked {} taskinstances due for execution", pickedExecutions.size());

        // Shared indicator for if there are more due executions in the database.
        // As soon as we know there are not more executions in the database, we can stop triggering checks for more (and vice versa)
        moreExecutionsInDatabase.set(pickedExecutions.size() == executionsToFetch);

        if (pickedExecutions.size() == 0) {
            // No picked executions to execute
            LOG.trace("No executions due.");
            return;
        }

        for (Execution picked : pickedExecutions) {
            scheduler.executor.addToQueue(
                new ExecutePicked(scheduler, picked),
                () -> {
                    if (moreExecutionsInDatabase.get()
                        && scheduler.executor.getNumberInQueueOrProcessing() <= lowerLimit) {
                        scheduler.triggerCheckForDueExecutions();
                    }
                });
        }
        scheduler.statsRegistry.register(StatsRegistry.SchedulerStatsEvent.RAN_EXECUTE_DUE);
    }
}
