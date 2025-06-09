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

import com.github.kagkarlsson.scheduler.event.ExecutionInterceptor;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.CandidateEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.logging.ConfigurableLogger;
import com.github.kagkarlsson.scheduler.task.Execution;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchCandidates implements PollStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(FetchCandidates.class);
  private final List<Executor> executors;
  private final Executor defaultExecutor;
  private final TaskRepository taskRepository;
  private final SchedulerClient schedulerClient;
  private final SchedulerListeners schedulerListeners;
  private final List<ExecutionInterceptor> executionInterceptors;
  private final SchedulerState schedulerState;
  private final ConfigurableLogger failureLogger;
  private final TaskResolver taskResolver;
  private final Clock clock;
  private final Runnable triggerCheckForNewExecutions;
  private final HeartbeatConfig heartbeatConfig;
  AtomicInteger currentGenerationNumber = new AtomicInteger(0);
  private final int lowerLimit;
  private final int upperLimit;

  public FetchCandidates(
      Executor defaultExecutor,
      TaskRepository taskRepository,
      SchedulerClient schedulerClient,
      SchedulerListeners schedulerListeners,
      List<ExecutionInterceptor> executionInterceptors,
      SchedulerState schedulerState,
      ConfigurableLogger failureLogger,
      TaskResolver taskResolver,
      Clock clock,
      Runnable triggerCheckForNewExecutions,
      HeartbeatConfig heartbeatConfig) {
    this(
        List.of(defaultExecutor),
        taskRepository,
        schedulerClient,
        schedulerListeners,
        executionInterceptors,
        schedulerState,
        failureLogger,
        taskResolver,
        clock,
        triggerCheckForNewExecutions,
        heartbeatConfig);
  }

  public FetchCandidates(
      List<Executor> executors,
      TaskRepository taskRepository,
      SchedulerClient schedulerClient,
      SchedulerListeners schedulerListeners,
      List<ExecutionInterceptor> executionInterceptors,
      SchedulerState schedulerState,
      ConfigurableLogger failureLogger,
      TaskResolver taskResolver,
      Clock clock,
      Runnable triggerCheckForNewExecutions,
      HeartbeatConfig heartbeatConfig) {
    this.executors = executors;
    this.defaultExecutor = executors.get(0);
    this.taskRepository = taskRepository;
    this.schedulerClient = schedulerClient;
    this.schedulerListeners = schedulerListeners;
    this.executionInterceptors = executionInterceptors;
    this.schedulerState = schedulerState;
    this.failureLogger = failureLogger;
    this.taskResolver = taskResolver;
    this.clock = clock;
    this.triggerCheckForNewExecutions = triggerCheckForNewExecutions;
    this.heartbeatConfig = heartbeatConfig;
    // Derive limits from per-pool limits
    lowerLimit = executors.stream().mapToInt(Executor::getPoolLowerLimit).sum();
    upperLimit = executors.stream().mapToInt(Executor::getPoolUpperLimit).sum();
  }

  private Executor findExecutorForExecution(Execution execution) {
    return ExecutorSelector.findExecutorForExecution(execution, executors, defaultExecutor);
  }

  @Override
  public void run() {
    Instant now = clock.now();

    // Fetch new candidates for execution. Old ones still in ExecutorService will become stale and
    // be discarded
    final int executionsToFetch = upperLimit;
    List<Execution> fetchedDueExecutions = taskRepository.getDue(now, executionsToFetch);
    LOG.trace(
        "Fetched {} task instances due for execution at {}", fetchedDueExecutions.size(), now);

    currentGenerationNumber.incrementAndGet();
    DueExecutionsBatch newDueBatch =
        new DueExecutionsBatch(
            currentGenerationNumber.get(),
            fetchedDueExecutions.size(),
            executionsToFetch == fetchedDueExecutions.size(),
            (Integer leftInBatch) -> leftInBatch <= lowerLimit);

    for (Execution e : fetchedDueExecutions) {
      Executor selectedExecutor = findExecutorForExecution(e);
      selectedExecutor.addToQueue(
          () -> {
            final Optional<Execution> candidate = new PickDue(e, newDueBatch).call();
            candidate.ifPresent(
                picked ->
                    new ExecutePicked(
                            selectedExecutor,
                            taskRepository,
                            schedulerClient,
                            schedulerListeners,
                            executionInterceptors,
                            taskResolver,
                            schedulerState,
                            failureLogger,
                            clock,
                            heartbeatConfig,
                            picked)
                        .run());
          },
          () -> newDueBatch.oneExecutionDone(triggerCheckForNewExecutions));
    }
    schedulerListeners.onSchedulerEvent(SchedulerEventType.RAN_EXECUTE_DUE);
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
      if (schedulerState.isShuttingDown()) {
        LOG.info(
            "Scheduler has been shutdown. Skipping fetched due execution: {}",
            candidate.taskInstance.getTaskAndInstance());
        return Optional.empty();
      }

      if (addedDueExecutionsBatch.isOlderGenerationThan(currentGenerationNumber.get())) {
        // skipping execution due to it being stale
        addedDueExecutionsBatch.markBatchAsStale();
        schedulerListeners.onCandidateEvent(CandidateEventType.STALE);
        LOG.trace(
            "Skipping queued execution (current generationNumber: {}, execution generationNumber: {})",
            currentGenerationNumber,
            addedDueExecutionsBatch.getGenerationNumber());
        return Optional.empty();
      }

      final Optional<Execution> pickedExecution = taskRepository.pick(candidate, clock.now());

      if (pickedExecution.isEmpty()) {
        // someone else picked id
        LOG.debug("Execution picked by another scheduler. Continuing to next due execution.");
        schedulerListeners.onCandidateEvent(CandidateEventType.ALREADY_PICKED);
        return Optional.empty();
      }

      return pickedExecution;
    }
  }
}
