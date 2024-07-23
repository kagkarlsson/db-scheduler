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
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.logging.ConfigurableLogger;
import com.github.kagkarlsson.scheduler.task.Execution;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockAndFetchCandidates implements PollStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(LockAndFetchCandidates.class);
  private final Executor executor;
  private final TaskRepository taskRepository;
  private final SchedulerClient schedulerClient;
  private final SchedulerListeners schedulerListeners;
  private final List<ExecutionInterceptor> executionInterceptors;
  private final TaskResolver taskResolver;
  private final SchedulerState schedulerState;
  private final ConfigurableLogger failureLogger;
  private final Clock clock;
  private final PollingStrategyConfig pollingStrategyConfig;
  private final Runnable triggerCheckForNewExecutions;
  private HeartbeatConfig maxAgeBeforeConsideredDead;
  private final int lowerLimit;
  private final int upperLimit;
  private AtomicBoolean moreExecutionsInDatabase = new AtomicBoolean(false);

  public LockAndFetchCandidates(
      Executor executor,
      TaskRepository taskRepository,
      SchedulerClient schedulerClient,
      int threadpoolSize,
      SchedulerListeners schedulerListeners,
      List<ExecutionInterceptor> executionInterceptors,
      SchedulerState schedulerState,
      ConfigurableLogger failureLogger,
      TaskResolver taskResolver,
      Clock clock,
      PollingStrategyConfig pollingStrategyConfig,
      Runnable triggerCheckForNewExecutions,
      HeartbeatConfig maxAgeBeforeConsideredDead) {
    this.executor = executor;
    this.taskRepository = taskRepository;
    this.schedulerClient = schedulerClient;
    this.schedulerListeners = schedulerListeners;
    this.executionInterceptors = executionInterceptors;
    this.taskResolver = taskResolver;
    this.schedulerState = schedulerState;
    this.failureLogger = failureLogger;
    this.clock = clock;
    this.pollingStrategyConfig = pollingStrategyConfig;
    this.triggerCheckForNewExecutions = triggerCheckForNewExecutions;
    this.maxAgeBeforeConsideredDead = maxAgeBeforeConsideredDead;
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
    // As soon as we know there are not more executions in the database, we can stop triggering
    // checks for more (and vice versa)
    moreExecutionsInDatabase.set(pickedExecutions.size() == executionsToFetch);

    if (pickedExecutions.size() == 0) {
      // No picked executions to execute
      LOG.trace("No executions due.");
      return;
    }

    for (Execution picked : pickedExecutions) {
      executor.addToQueue(
          new ExecutePicked(
              executor,
              taskRepository,
              schedulerClient,
              schedulerListeners,
              executionInterceptors,
              taskResolver,
              schedulerState,
              failureLogger,
              clock,
              maxAgeBeforeConsideredDead,
              picked),
          () -> {
            if (moreExecutionsInDatabase.get()
                && executor.getNumberInQueueOrProcessing() <= lowerLimit) {
              triggerCheckForNewExecutions.run();
            }
          });
    }
    schedulerListeners.onSchedulerEvent(SchedulerEventType.RAN_EXECUTE_DUE);
  }
}
