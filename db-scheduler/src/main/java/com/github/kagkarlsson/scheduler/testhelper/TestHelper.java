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

import com.github.kagkarlsson.scheduler.PollingStrategyConfig;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.jdbc.DefaultJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistryAdapter;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;

public class TestHelper {

  public static ManualSchedulerBuilder createManualScheduler(
      DataSource dataSource, Task<?>... knownTasks) {
    return new ManualSchedulerBuilder(dataSource, Arrays.asList(knownTasks));
  }

  public static ManualSchedulerBuilder createManualScheduler(
      DataSource dataSource, List<Task<?>> knownTasks) {
    return new ManualSchedulerBuilder(dataSource, knownTasks);
  }

  public static class ManualSchedulerBuilder extends SchedulerBuilder {
    private SettableClock clock;

    protected List<SchedulerListener> schedulerListeners =
        List.of(new StatsRegistryAdapter(statsRegistry));

    public ManualSchedulerBuilder(DataSource dataSource, List<Task<?>> knownTasks) {
      super(dataSource, knownTasks);
    }

    public ManualSchedulerBuilder clock(SettableClock clock) {
      this.clock = clock;
      return this;
    }

    public <T extends Task<?> & OnStartup> ManualSchedulerBuilder startTasks(List<T> startTasks) {
      super.startTasks(startTasks);
      return this;
    }

    public ManualSchedulerBuilder statsRegistry(StatsRegistry statsRegistry) {
      super.statsRegistry = statsRegistry;
      return this;
    }

    public ManualSchedulerBuilder pollingStrategy(PollingStrategyConfig pollingStrategyConfig) {
      super.pollingStrategyConfig = pollingStrategyConfig;
      return this;
    }

    public ManualSchedulerBuilder addSchedulerListener(SchedulerListener schedulerListener) {
      schedulerListeners =
          Stream.concat(schedulerListeners.stream(), Stream.of(schedulerListener))
              .collect(Collectors.toList());
      return this;
    }

    public ManualScheduler build() {
      final TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, knownTasks);
      final JdbcTaskRepository schedulerTaskRepository =
          new JdbcTaskRepository(
              dataSource,
              true,
              new DefaultJdbcCustomization(false),
              tableName,
              taskResolver,
              new SchedulerName.Fixed("manual"),
              serializer,
              enablePriority,
              clock);
      final JdbcTaskRepository clientTaskRepository =
          new JdbcTaskRepository(
              dataSource,
              commitWhenAutocommitDisabled,
              new DefaultJdbcCustomization(false),
              tableName,
              taskResolver,
              new SchedulerName.Fixed("manual"),
              serializer,
              enablePriority,
              clock);

      return new ManualScheduler(
          clock,
          schedulerTaskRepository,
          clientTaskRepository,
          taskResolver,
          executorThreads,
          new DirectExecutorService(),
          schedulerName,
          waiter,
          heartbeatInterval,
          enableImmediateExecution,
          schedulerListeners,
          Optional.ofNullable(pollingStrategyConfig).orElse(PollingStrategyConfig.DEFAULT_FETCH),
          deleteUnresolvedAfter,
          LogLevel.DEBUG,
          true,
          startTasks,
          new ThrowingScheduledExecutorService(),
          new ThrowingScheduledExecutorService(),
          false);
    }

    public ManualScheduler start() {
      ManualScheduler scheduler = build();
      scheduler.start();
      return scheduler;
    }
  }

  private abstract static class BaseExecutorService extends AbstractExecutorService {

    @Override
    public void shutdown() {}

    @Override
    public List<Runnable> shutdownNow() {
      return new ArrayList<>();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }
  }

  private static class DirectExecutorService extends BaseExecutorService {

    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }

  private static class ThrowingScheduledExecutorService extends BaseExecutorService
      implements ScheduledExecutorService {

    private RuntimeException doNotUseError() {
      return new RuntimeException(
          "This ExecutorService should never be used. "
              + "The intention is to trigger the desired scheduler-operation manually, e.g scheduler.runAnyDueExecutions()");
    }

    @Override
    public void execute(Runnable command) {
      throw doNotUseError();
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      throw doNotUseError();
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      throw doNotUseError();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
        Runnable command, long initialDelay, long period, TimeUnit unit) {
      throw doNotUseError();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
        Runnable command, long initialDelay, long delay, TimeUnit unit) {
      throw doNotUseError();
    }
  }
}
