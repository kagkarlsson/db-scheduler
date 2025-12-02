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

import com.github.kagkarlsson.scheduler.task.Execution;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executor {
  private static final Logger LOG = LoggerFactory.getLogger(Executor.class);

  final Map<UUID, CurrentlyExecuting> currentlyProcessing =
      Collections.synchronizedMap(new HashMap<>());
  private final AtomicInteger currentlyInQueueOrProcessing = new AtomicInteger(0);
  private final ExecutorService executorService;
  private final Clock clock;
  private final int priorityThreshold;
  private final AtomicInteger tasksExecuted = new AtomicInteger(0);

  public Executor(ExecutorService executorService, Clock clock) {
    this(executorService, clock, Integer.MIN_VALUE);
  }

  public Executor(ExecutorService executorService, Clock clock, int priorityThreshold) {
    this.executorService = executorService;
    this.clock = clock;
    this.priorityThreshold = priorityThreshold;
  }

  public void addToQueue(Runnable r, Runnable afterDone) {
    currentlyInQueueOrProcessing
        .incrementAndGet(); // if we always had a ThreadPoolExecutor we could check queue-size using
    // getQueue()
    executorService.execute(
        () -> {
          // Execute
          try {
            r.run();
            tasksExecuted.incrementAndGet(); // Track tasks executed by this pool
          } finally {
            currentlyInQueueOrProcessing.decrementAndGet();
            // Run callbacks after decrementing currentlyInQueueOrProcessing
            afterDone.run();
          }
        });
  }

  public List<CurrentlyExecuting> getCurrentlyExecuting() {
    return new ArrayList<>(currentlyProcessing.values());
  }

  public void stop(Duration shutdownMaxWait) {
    LOG.info("Letting running executions finish. Will wait up to 2x{}.", shutdownMaxWait);
    final Instant startShutdown = clock.now();
    if (ExecutorUtils.shutdownAndAwaitTermination(
        executorService, shutdownMaxWait, shutdownMaxWait)) {
      LOG.info("Scheduler stopped.");
    } else {
      LOG.warn(
          "Scheduler stopped, but some tasks did not complete. Was currently running the following executions:\n{}",
          new ArrayList<>(currentlyProcessing.values())
              .stream()
                  .map(CurrentlyExecuting::getExecution)
                  .map(Execution::toString)
                  .collect(Collectors.joining("\n")));
    }

    final Duration shutdownTime = Duration.between(startShutdown, clock.now());
    if (shutdownMaxWait.toMillis() > Duration.ofMinutes(1).toMillis()
        && shutdownTime.toMillis() >= shutdownMaxWait.toMillis()) {
      LOG.info(
          "Shutdown of the scheduler executor service took {}. Consider regularly checking for "
              + "'executionContext.getSchedulerState().isShuttingDown()' in task execution-handler and abort when "
              + "scheduler is shutting down.",
          shutdownTime);
    }
  }

  public int getNumberInQueueOrProcessing() {
    return currentlyInQueueOrProcessing.get();
  }

  public UUID addCurrentlyProcessing(CurrentlyExecuting currentlyExecuting) {
    final UUID executionId = UUID.randomUUID();
    currentlyProcessing.put(executionId, currentlyExecuting);
    return executionId;
  }

  public boolean canExecute(Execution execution) {
    return execution.taskInstance.getPriority() >= priorityThreshold;
  }

  public int getPriorityThreshold() {
    return priorityThreshold;
  }

  public int getTasksExecuted() {
    return tasksExecuted.get();
  }

  /**
   * Check if this executor has capacity to accept more tasks. This is a best-effort check that
   * works with ThreadPoolExecutor.
   *
   * @return true if the executor likely has capacity, false if it's likely at capacity
   */
  public boolean hasCapacity() {
    if (executorService instanceof java.util.concurrent.ThreadPoolExecutor) {
      ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
      int maximumPoolSize = threadPoolExecutor.getMaximumPoolSize();
      int activeThreads = threadPoolExecutor.getActiveCount();

      // Executor has capacity if not all threads are active
      return activeThreads < maximumPoolSize;
    }

    // For other ExecutorService implementations, fall back to a conservative approach
    // Assume it has capacity unless we know it's very busy
    return getNumberInQueueOrProcessing() < 10; // Conservative threshold
  }

  public void removeCurrentlyProcessing(UUID executionId) {
    if (currentlyProcessing.remove(executionId) == null) {
      LOG.warn(
          "Released execution was not found in collection of executions currently being processed. Should never happen. Execution-id: "
              + executionId);
    }
  }
}
