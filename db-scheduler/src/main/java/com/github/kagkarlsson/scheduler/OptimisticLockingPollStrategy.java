package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

public class OptimisticLockingPollStrategy implements PollStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(OptimisticLockingPollStrategy.class);
    private final Scheduler scheduler;
    private final PollingStrategyConfig pollingStrategyConfig;
    int currentGenerationNumber = 1;

    public OptimisticLockingPollStrategy(Scheduler scheduler, PollingStrategyConfig pollingStrategyConfig) {
        this.scheduler = scheduler;
        this.pollingStrategyConfig = pollingStrategyConfig;
    }

    @Override
    public void run() {
        Instant now = scheduler.clock.now();
        int lowerLimit = pollingStrategyConfig.getLowerLimit(scheduler.threadpoolSize);
        int upperLimit = pollingStrategyConfig.getUpperLimit(scheduler.threadpoolSize);

        List<Execution> dueExecutions = scheduler.taskRepository.getDue(now, upperLimit);
        LOG.trace("Found {} taskinstances due for execution", dueExecutions.size());

        int thisGenerationNumber = currentGenerationNumber + 1;
        DueExecutionsBatch newDueBatch = new DueExecutionsBatch(
            scheduler.threadpoolSize,
            thisGenerationNumber,
            dueExecutions.size(),
            upperLimit == dueExecutions.size(),
            actualExecutionsLeftInBatch -> actualExecutionsLeftInBatch <= lowerLimit);

        for (Execution e : dueExecutions) {
            scheduler.executorService.execute(() -> {
                Optional<Execution> candidate = new PickDue(e, newDueBatch).call();
                candidate.ifPresent(picked -> new ExecutePicked(scheduler, picked).run());
                newDueBatch.oneExecutionDone(scheduler::triggerCheckForDueExecutions);
            });
        }
        currentGenerationNumber = thisGenerationNumber;
        scheduler.statsRegistry.register(StatsRegistry.SchedulerStatsEvent.RAN_EXECUTE_DUE);

    }

    private class PickDue implements Callable<Optional<Execution>> {
        private Execution candidate;
        private DueExecutionsBatch addedDueExecutionsBatch;

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

            final Optional<Execution> pickedExecution = scheduler.taskRepository.pick(candidate, scheduler.clock.now());

            if (!pickedExecution.isPresent()) {
                // someone else picked id
                LOG.debug("Execution picked by another scheduler. Continuing to next due execution.");
                scheduler.statsRegistry.register(StatsRegistry.CandidateStatsEvent.ALREADY_PICKED);
            }

            return pickedExecution;
        }
    }

}
