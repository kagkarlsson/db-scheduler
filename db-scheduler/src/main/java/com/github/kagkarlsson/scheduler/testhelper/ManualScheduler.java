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
package com.github.kagkarlsson.scheduler.testhelper;

import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManualScheduler extends Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(ManualScheduler.class);
  private final SettableClock clock;

  ManualScheduler(
      SettableClock clock,
      TaskRepository schedulerTaskRepository,
      TaskRepository clientTaskRepository,
      TaskResolver taskResolver,
      int maxThreads,
      ExecutorService executorService,
      SchedulerName schedulerName,
      Waiter waiter,
      Duration heartbeatInterval,
      boolean executeImmediately,
      StatsRegistry statsRegistry,
      PollingStrategyConfig pollingStrategyConfig,
      Duration deleteUnresolvedAfter,
      LogLevel logLevel,
      boolean logStackTrace,
      List<OnStartup> onStartup,
      ExecutorService dueExecutor,
      ScheduledExecutorService houseKeeperExecutor) {
    super(
        clock,
        schedulerTaskRepository,
        clientTaskRepository,
        taskResolver,
        maxThreads,
        executorService,
        schedulerName,
        waiter,
        heartbeatInterval,
        SchedulerBuilder.DEFAULT_MISSED_HEARTBEATS_LIMIT,
        executeImmediately,
        statsRegistry,
        pollingStrategyConfig,
        deleteUnresolvedAfter,
        Duration.ZERO,
        logLevel,
        logStackTrace,
        onStartup,
        dueExecutor,
        houseKeeperExecutor);
    this.clock = clock;
  }

  public SettableClock getClock() {
    return clock;
  }

  public void tick(Duration moveClockForward) {
    clock.set(clock.now.plus(moveClockForward));
  }

  public void setTime(Instant newtime) {
    clock.set(newtime);
  }

  public void runAnyDueExecutions() {
    super.executeDueStrategy.run();
  }

  public void runDeadExecutionDetection() {
    super.detectDeadExecutions();
  }

  public void start() {
    LOG.info("Starting manual scheduler. Executing on-startup tasks.");
    executeOnStartup();
  }
}
