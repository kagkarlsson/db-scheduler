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

import com.github.kagkarlsson.scheduler.logging.ConfigurableLogger;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.AsyncExecutionHandler;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class LockAndFetchCandidates implements PollStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(LockAndFetchCandidates.class);
    private final Executor executor;
    private final TaskRepository taskRepository;
    private final SchedulerClient schedulerClient;
    private SchedulerClientEventListener earlyExecutionListener;
    private final StatsRegistry statsRegistry;
    private final TaskResolver taskResolver;
    private final SchedulerState schedulerState;
    private final ConfigurableLogger failureLogger;
    private final Clock clock;
    private final PollingStrategyConfig pollingStrategyConfig;
    private final Runnable triggerCheckForNewExecutions;
    private final int lowerLimit;
    private final int upperLimit;
    private AtomicBoolean moreExecutionsInDatabase = new AtomicBoolean(false);

    public LockAndFetchCandidates(Executor executor, TaskRepository taskRepository, SchedulerClient schedulerClient,
                                  SchedulerClientEventListener earlyExecutionListener, int threadpoolSize, StatsRegistry statsRegistry, SchedulerState schedulerState,
                                  ConfigurableLogger failureLogger, TaskResolver taskResolver, Clock clock,
                                  PollingStrategyConfig pollingStrategyConfig, Runnable triggerCheckForNewExecutions) {
        this.executor = executor;
        this.taskRepository = taskRepository;
        this.schedulerClient = schedulerClient;
        this.earlyExecutionListener = earlyExecutionListener;
        this.statsRegistry = statsRegistry;
        this.taskResolver = taskResolver;
        this.schedulerState = schedulerState;
        this.failureLogger = failureLogger;
        this.clock = clock;
        this.pollingStrategyConfig = pollingStrategyConfig;
        this.triggerCheckForNewExecutions = triggerCheckForNewExecutions;
        lowerLimit = pollingStrategyConfig.getLowerLimit(threadpoolSize);
        upperLimit = pollingStrategyConfig.getUpperLimit(threadpoolSize);
    }

    @Override
    public void run() {
        Instant now = clock.now();

        int executionsToFetch = upperLimit - executor.getNumberInQueueOrProcessing();

        // Might happen if upperLimit == threads and all threads are busy
        if (executionsToFetch <= 0) {
            LOG.trace("No executions to fetch.");
            return;
        }

        // FIXLATER: should it fetch here if not under lowerLimit? probably
        List<Execution> pickedExecutions = taskRepository.lockAndGetDue(now, executionsToFetch);
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
            CompletableFuture<Void> future = CompletableFuture
                .runAsync(executor::incrementInQueue, executor.getExecutorService())
                .thenComposeAsync((_ignored) -> {
                    // Experimental support for async execution. Peek at Task to see if support async
                    // Unresolved tasks will be handled further in
                    final Optional<Task> task = taskResolver.resolve(picked.taskInstance.getTaskName());
                    if (task.isPresent() && task.get() instanceof AsyncExecutionHandler) {

                        return new AsyncExecutePicked(executor, taskRepository, earlyExecutionListener,
                            schedulerClient, statsRegistry, taskResolver, schedulerState, failureLogger,
                            clock, picked).toCompletableFuture();
                    } else {

                        return CompletableFuture.runAsync(new ExecutePicked(executor, taskRepository, earlyExecutionListener,
                            schedulerClient, statsRegistry, taskResolver, schedulerState, failureLogger,
                            clock, picked), executor.getExecutorService());
                    }

                }, executor.getExecutorService())
                .thenAccept(x -> {
                    executor.decrementInQueue();
                    if (moreExecutionsInDatabase.get()
                        && executor.getNumberInQueueOrProcessing() <= lowerLimit) {
                        triggerCheckForNewExecutions.run();
                    }
                });

            executor.addOngoingWork(future);
        }
        statsRegistry.register(StatsRegistry.SchedulerStatsEvent.RAN_EXECUTE_DUE);
    }
}
