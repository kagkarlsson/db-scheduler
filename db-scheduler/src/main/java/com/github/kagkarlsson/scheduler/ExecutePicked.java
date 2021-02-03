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
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

@SuppressWarnings("rawtypes")
class ExecutePicked implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutePicked.class);
    private final Scheduler scheduler;
    private final Execution pickedExecution;

    public ExecutePicked(Scheduler scheduler, Execution pickedExecution) {
        this.scheduler = scheduler;
        this.pickedExecution = pickedExecution;
    }

    @Override
    public void run() {
        scheduler.currentlyProcessing.put(pickedExecution, new CurrentlyExecuting(pickedExecution, scheduler.clock));
        try {
            scheduler.statsRegistry.register(StatsRegistry.CandidateStatsEvent.EXECUTED);
            executePickedExecution(pickedExecution);
        } finally {
            if (scheduler.currentlyProcessing.remove(pickedExecution) == null) {
                // May happen in rare circumstances (typically concurrency tests)
                // TODO: generate a unique-id for this specific run, add information of what thread is executing if possible
                LOG.warn("Released execution was not found in collection of executions currently being processed. Should never happen.");
            }
        }
    }

    private void executePickedExecution(Execution execution) {
        final Optional<Task> task = scheduler.taskResolver.resolve(execution.taskInstance.getTaskName());
        if (!task.isPresent()) {
            LOG.error("Failed to find implementation for task with name '{}'. Should have been excluded in JdbcRepository.", execution.taskInstance.getTaskName());
            scheduler.statsRegistry.register(StatsRegistry.SchedulerStatsEvent.UNEXPECTED_ERROR);
            return;
        }

        Instant executionStarted = scheduler.clock.now();
        try {
            LOG.debug("Executing " + execution);
            CompletionHandler completion = task.get().execute(execution.taskInstance, new ExecutionContext(scheduler.schedulerState, execution, scheduler));
            LOG.debug("Execution done");

            complete(completion, execution, executionStarted);
            scheduler.statsRegistry.register(StatsRegistry.ExecutionStatsEvent.COMPLETED);

        } catch (RuntimeException unhandledException) {
            LOG.error("Unhandled exception during execution of task with name '{}'. Treating as failure.", task.get().getName(), unhandledException);
            failure(task.get().getFailureHandler(), execution, unhandledException, executionStarted);
            scheduler.statsRegistry.register(StatsRegistry.ExecutionStatsEvent.FAILED);

        } catch (Throwable unhandledError) {
            LOG.error("Error during execution of task with name '{}'. Treating as failure.", task.get().getName(), unhandledError);
            failure(task.get().getFailureHandler(), execution, unhandledError, executionStarted);
            scheduler.statsRegistry.register(StatsRegistry.ExecutionStatsEvent.FAILED);
        }
    }

    private void complete(CompletionHandler completion, Execution execution, Instant executionStarted) {
        ExecutionComplete completeEvent = ExecutionComplete.success(execution, executionStarted, scheduler.clock.now());
        try {
            completion.complete(completeEvent, new ExecutionOperations(scheduler.schedulerTaskRepository, execution));
            scheduler.statsRegistry.registerSingleCompletedExecution(completeEvent);
        } catch (Throwable e) {
            scheduler.statsRegistry.register(StatsRegistry.SchedulerStatsEvent.COMPLETIONHANDLER_ERROR);
            scheduler.statsRegistry.register(StatsRegistry.SchedulerStatsEvent.UNEXPECTED_ERROR);
            LOG.error("Failed while completing execution {}. Execution will likely remain scheduled and locked/picked. " +
                "The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, scheduler.getMaxAgeBeforeConsideredDead(), e);
        }
    }

    private void failure(FailureHandler failureHandler, Execution execution, Throwable cause, Instant executionStarted) {
        ExecutionComplete completeEvent = ExecutionComplete.failure(execution, executionStarted, scheduler.clock.now(), cause);
        try {
            failureHandler.onFailure(completeEvent, new ExecutionOperations(scheduler.schedulerTaskRepository, execution));
            scheduler.statsRegistry.registerSingleCompletedExecution(completeEvent);
        } catch (Throwable e) {
            scheduler.statsRegistry.register(StatsRegistry.SchedulerStatsEvent.FAILUREHANDLER_ERROR);
            scheduler.statsRegistry.register(StatsRegistry.SchedulerStatsEvent.UNEXPECTED_ERROR);
            LOG.error("Failed while completing execution {}. Execution will likely remain scheduled and locked/picked. " +
                "The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, scheduler.getMaxAgeBeforeConsideredDead(), e);
        }
    }
}
