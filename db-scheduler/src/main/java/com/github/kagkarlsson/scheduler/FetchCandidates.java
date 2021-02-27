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
import java.util.Optional;
import java.util.concurrent.Callable;

public class FetchCandidates implements PollStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(FetchCandidates.class);
    private final Scheduler scheduler;
    private final PollingStrategyConfig pollingStrategyConfig;
    int currentGenerationNumber = 1;
    private final int lowerLimit;
    private final int upperLimit;

    public FetchCandidates(Scheduler scheduler, PollingStrategyConfig pollingStrategyConfig) {
        this.scheduler = scheduler;
        this.pollingStrategyConfig = pollingStrategyConfig;
        lowerLimit = pollingStrategyConfig.getLowerLimit(scheduler.threadpoolSize);
        //FIXLATER: this is not "upper limit", but rather nr of executions to get. those already in queue will become stale
        upperLimit = pollingStrategyConfig.getUpperLimit(scheduler.threadpoolSize);
    }

    @Override
    public void run() {
        Instant now = scheduler.clock.now();

        // Fetch new candidates for execution. Old ones still in ExecutorService will become stale and be discarded
        final int executionsToFetch = upperLimit;
        List<Execution> fetchedDueExecutions = scheduler.schedulerTaskRepository.getDue(now, executionsToFetch);
        LOG.trace("Fetched {} task instances due for execution", fetchedDueExecutions.size());

        this.currentGenerationNumber = this.currentGenerationNumber + 1;
        DueExecutionsBatch newDueBatch = new DueExecutionsBatch(
            currentGenerationNumber,
            fetchedDueExecutions.size(),
            executionsToFetch == fetchedDueExecutions.size(),
            (Integer leftInBatch) -> leftInBatch <= lowerLimit);

        for (Execution e : fetchedDueExecutions) {
            scheduler.executor.addToQueue(
                () -> {
                    final Optional<Execution> candidate = new PickDue(e, newDueBatch).call();
                    candidate.ifPresent(picked -> new ExecutePicked(scheduler, picked).run());
                },
                () -> {
                    newDueBatch.oneExecutionDone(scheduler::triggerCheckForDueExecutions);
                });
        }
        scheduler.statsRegistry.register(StatsRegistry.SchedulerStatsEvent.RAN_EXECUTE_DUE);
    }

    private class PickDue implements Callable<Optional<Execution>> {
        private final Execution candidate;
        private final DueExecutionsBatch addedDueExecutionsBatch;

        public PickDue(Execution candidate, DueExecutionsBatch dueExecutionsBatch) {
            this.candidate = candidate;
            this.addedDueExecutionsBatch = dueExecutionsBatch;
        }

        @Override
        public Optional<Execution> call() {
            if (scheduler.schedulerState.isShuttingDown()) {
                LOG.info("Scheduler has been shutdown. Skipping fetched due execution: " + candidate.taskInstance.getTaskAndInstance());
                return Optional.empty();
            }

            if (addedDueExecutionsBatch.isOlderGenerationThan(currentGenerationNumber)) {
                // skipping execution due to it being stale
                addedDueExecutionsBatch.markBatchAsStale();
                scheduler.statsRegistry.register(StatsRegistry.CandidateStatsEvent.STALE);
                LOG.trace("Skipping queued execution (current generationNumber: {}, execution generationNumber: {})", currentGenerationNumber, addedDueExecutionsBatch.getGenerationNumber());
                return Optional.empty();
            }

            final Optional<Execution> pickedExecution = scheduler.schedulerTaskRepository.pick(candidate, scheduler.clock.now());

            if (!pickedExecution.isPresent()) {
                // someone else picked id
                LOG.debug("Execution picked by another scheduler. Continuing to next due execution.");
                scheduler.statsRegistry.register(StatsRegistry.CandidateStatsEvent.ALREADY_PICKED);
                return Optional.empty();
            }

            return pickedExecution;
        }

    }

}
