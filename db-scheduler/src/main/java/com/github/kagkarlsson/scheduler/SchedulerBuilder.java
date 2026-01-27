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

import static com.github.kagkarlsson.scheduler.ExecutorUtils.defaultThreadFactoryWithPrefix;
import static com.github.kagkarlsson.scheduler.Scheduler.THREAD_PREFIX;
import static java.util.Optional.ofNullable;

import com.github.kagkarlsson.scheduler.event.ExecutionInterceptor;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistryAdapter;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerBuilder {
  public static final double UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH = 3.0;
  public static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(10);
  public static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofMinutes(5);
  public static final int DEFAULT_MISSED_HEARTBEATS_LIMIT = 6;
  public static final Duration DEFAULT_DELETION_OF_UNRESOLVED_TASKS_DURATION = Duration.ofDays(14);
  public static final Duration SHUTDOWN_MAX_WAIT = Duration.ofMinutes(30);
  public static final PollingStrategyConfig DEFAULT_POLLING_STRATEGY =
      new PollingStrategyConfig(
          PollingStrategyConfig.Type.FETCH, 0.5, UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH);
  public static final LogLevel DEFAULT_FAILURE_LOG_LEVEL = LogLevel.WARN;
  public static final boolean LOG_STACK_TRACE_ON_FAILURE = true;
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerBuilder.class);
  protected final DataSource dataSource;
  protected final List<Task<?>> knownTasks = new ArrayList<>();
  protected final List<OnStartup> startTasks = new ArrayList<>();
  protected Clock clock = new SystemClock(); // if this is set, waiter-clocks must be updated
  protected SchedulerName schedulerName;
  protected int executorThreads = 10;
  protected Duration poolingInterval = DEFAULT_POLLING_INTERVAL;
  protected StatsRegistry statsRegistry = StatsRegistry.NOOP;
  protected Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
  protected Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
  protected String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;
  protected boolean enableImmediateExecution = false;
  protected ExecutorService executorService;
  protected ExecutorService dueExecutor;
  protected ScheduledExecutorService housekeeperExecutor;
  protected Duration deleteUnresolvedAfter = DEFAULT_DELETION_OF_UNRESOLVED_TASKS_DURATION;
  protected JdbcCustomization jdbcCustomization = null;
  protected Duration shutdownMaxWait = SHUTDOWN_MAX_WAIT;
  protected boolean commitWhenAutocommitDisabled = false;
  protected PollingStrategyConfig pollingStrategyConfig = DEFAULT_POLLING_STRATEGY;
  protected LogLevel logLevel = DEFAULT_FAILURE_LOG_LEVEL;
  protected boolean logStackTrace = LOG_STACK_TRACE_ON_FAILURE;
  protected boolean enablePriority = false;
  protected boolean registerShutdownHook = false;
  protected int numberOfMissedHeartbeatsBeforeDead = DEFAULT_MISSED_HEARTBEATS_LIMIT;
  protected boolean alwaysPersistTimestampInUTC = false;
  protected List<SchedulerListener> schedulerListeners = new ArrayList<>();
  protected List<ExecutionInterceptor> executionInterceptors = new ArrayList<>();
  protected List<WorkerPoolConfig> workerPools = new ArrayList<>();

  protected record WorkerPoolConfig(
      int threads, int priorityThreshold, ExecutorService executorService) {}

  private int getWorkerPoolThreads(WorkerPoolConfig config) {
    if (config.threads() > 0) {
      return config.threads();
    }
    // Try to extract from ExecutorService
    if (config.executorService() instanceof java.util.concurrent.ThreadPoolExecutor tpe) {
      return tpe.getMaximumPoolSize();
    }
    throw new IllegalStateException(
        "Cannot determine thread count for custom ExecutorService in worker pool. "
            + "Please use addWorkerPool(int threads, int priorityThreshold, ExecutorService) instead.");
  }

  public SchedulerBuilder(DataSource dataSource, List<Task<?>> knownTasks) {
    this.dataSource = dataSource;
    this.knownTasks.addAll(knownTasks);
  }

  @SafeVarargs
  public final <T extends Task<?> & OnStartup> SchedulerBuilder startTasks(T... startTasks) {
    return startTasks(Arrays.asList(startTasks));
  }

  public <T extends Task<?> & OnStartup> SchedulerBuilder startTasks(List<T> startTasks) {
    knownTasks.addAll(startTasks);
    this.startTasks.addAll(startTasks);
    return this;
  }

  public SchedulerBuilder pollingInterval(Duration pollingInterval) {
    this.poolingInterval = pollingInterval;
    return this;
  }

  public SchedulerBuilder heartbeatInterval(Duration duration) {
    this.heartbeatInterval = duration;
    return this;
  }

  public SchedulerBuilder missedHeartbeatsLimit(int numberOfMissedHeartbeatsBeforeDead) {
    if (numberOfMissedHeartbeatsBeforeDead < 4) {
      throw new IllegalArgumentException("Heartbeat-limit must be at least 4");
    }
    this.numberOfMissedHeartbeatsBeforeDead = numberOfMissedHeartbeatsBeforeDead;
    return this;
  }

  public SchedulerBuilder threads(int numberOfThreads) {
    this.executorThreads = numberOfThreads;
    return this;
  }

  public SchedulerBuilder executorService(ExecutorService executorService) {
    this.executorService = executorService;
    return this;
  }

  public SchedulerBuilder dueExecutor(ExecutorService dueExecutor) {
    this.dueExecutor = dueExecutor;
    return this;
  }

  public SchedulerBuilder housekeeperExecutor(ScheduledExecutorService housekeeperExecutor) {
    this.housekeeperExecutor = housekeeperExecutor;
    return this;
  }

  /** Deprecated, use addSchedulerListener instead */
  @Deprecated
  public SchedulerBuilder statsRegistry(StatsRegistry statsRegistry) {
    this.statsRegistry = statsRegistry;
    return this;
  }

  public SchedulerBuilder addSchedulerListener(SchedulerListener schedulerListener) {
    this.schedulerListeners.add(schedulerListener);
    return this;
  }

  public SchedulerBuilder addExecutionInterceptor(ExecutionInterceptor interceptor) {
    this.executionInterceptors.add(interceptor);
    return this;
  }

  public SchedulerBuilder schedulerName(SchedulerName schedulerName) {
    this.schedulerName = schedulerName;
    return this;
  }

  public SchedulerBuilder serializer(Serializer serializer) {
    this.serializer = serializer;
    return this;
  }

  public SchedulerBuilder tableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public SchedulerBuilder enableImmediateExecution() {
    this.enableImmediateExecution = true;
    return this;
  }

  public SchedulerBuilder deleteUnresolvedAfter(Duration deleteAfter) {
    this.deleteUnresolvedAfter = deleteAfter;
    return this;
  }

  public SchedulerBuilder jdbcCustomization(JdbcCustomization jdbcCustomization) {
    this.jdbcCustomization = jdbcCustomization;
    return this;
  }

  public SchedulerBuilder alwaysPersistTimestampInUTC() {
    this.alwaysPersistTimestampInUTC = true;
    return this;
  }

  public SchedulerBuilder shutdownMaxWait(Duration shutdownMaxWait) {
    this.shutdownMaxWait = shutdownMaxWait;
    return this;
  }

  public SchedulerBuilder commitWhenAutocommitDisabled(boolean commitWhenAutocommitDisabled) {
    this.commitWhenAutocommitDisabled = commitWhenAutocommitDisabled;
    return this;
  }

  public SchedulerBuilder pollUsingFetchAndLockOnExecute(
      double lowerLimitFractionOfThreads, double executionsPerBatchFractionOfThreads) {
    this.pollingStrategyConfig =
        new PollingStrategyConfig(
            PollingStrategyConfig.Type.FETCH,
            lowerLimitFractionOfThreads,
            executionsPerBatchFractionOfThreads);
    return this;
  }

  public SchedulerBuilder pollUsingLockAndFetch(
      double lowerLimitFractionOfThreads, double upperLimitFractionOfThreads) {
    this.pollingStrategyConfig =
        new PollingStrategyConfig(
            PollingStrategyConfig.Type.LOCK_AND_FETCH,
            lowerLimitFractionOfThreads,
            upperLimitFractionOfThreads);
    return this;
  }

  public SchedulerBuilder failureLogging(LogLevel logLevel, boolean logStackTrace) {
    if (logLevel == null) {
      throw new IllegalArgumentException("Log level must not be null");
    }
    this.logLevel = logLevel;
    this.logStackTrace = logStackTrace;
    return this;
  }

  public SchedulerBuilder registerShutdownHook() {
    this.registerShutdownHook = true;
    return this;
  }

  public SchedulerBuilder enablePriority() {
    this.enablePriority = true;
    return this;
  }

  public SchedulerBuilder clock(Clock clock) {
    this.clock = clock;
    return this;
  }

  /**
   * Adds a worker pool that will only execute tasks with a priority greater than or equal to the
   * specified threshold.
   *
   * <p>Note: The scheduler's fetch limits (lowerLimit and upperLimit) are calculated based on the
   * TOTAL thread capacity across all pools. This ensures that when high-priority tasks are
   * available, all pools can be fully utilized.
   *
   * @param threads the number of threads in the worker pool
   * @param priorityThreshold the minimum priority of tasks that this worker pool will execute
   * @return this builder
   */
  public SchedulerBuilder addWorkerPool(int threads, int priorityThreshold) {
    if (threads <= 0) {
      throw new IllegalArgumentException("Number of threads must be greater than 0");
    }

    if (!enablePriority) {
      throw new IllegalStateException(
          "Priority must be enabled to add a worker pool with priority threshold");
    }

    ExecutorService executorService =
        Executors.newFixedThreadPool(
            threads,
            defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-p" + priorityThreshold + "-"));

    this.workerPools.add(new WorkerPoolConfig(threads, priorityThreshold, executorService));
    return this;
  }

  /**
   * Adds a worker pool that will only execute tasks with a priority greater than or equal to the
   * specified threshold, using a custom ExecutorService.
   *
   * <p>Note: The scheduler's fetch limits (lowerLimit and upperLimit) are calculated based on the
   * TOTAL thread capacity across all pools. For custom ExecutorService instances, the thread count
   * will be extracted from ThreadPoolExecutor.getMaximumPoolSize() if possible.
   *
   * @param executorService the executor service to use for this worker pool
   * @param priorityThreshold the minimum priority of tasks that this worker pool will execute
   * @return this builder
   */
  public SchedulerBuilder addWorkerPool(ThreadPoolExecutor executorService, int priorityThreshold) {
    if (executorService == null) {
      throw new IllegalArgumentException("ExecutorService must not be null");
    }

    if (!enablePriority) {
      throw new IllegalStateException(
          "Priority must be enabled to add a worker pool with priority threshold");
    }

    this.workerPools.add(
        new WorkerPoolConfig(
            executorService.getMaximumPoolSize(), priorityThreshold, executorService));
    return this;
  }

  /**
   * Adds a worker pool that will only execute tasks with a priority greater than or equal to the
   * specified threshold, using a custom ExecutorService with an explicit thread count.
   *
   * <p>Use this overload when providing a custom ExecutorService that is not a ThreadPoolExecutor,
   * or when you want to explicitly specify the thread count for limit calculations.
   *
   * <p>Note: The scheduler's fetch limits (lowerLimit and upperLimit) are calculated based on the
   * TOTAL thread capacity across all pools.
   *
   * @param threads the number of threads in the worker pool (used for limit calculations)
   * @param priorityThreshold the minimum priority of tasks that this worker pool will execute
   * @param executorService the executor service to use for this worker pool
   * @return this builder
   */
  public SchedulerBuilder addWorkerPool(
      int threads, int priorityThreshold, ExecutorService executorService) {
    if (threads <= 0) {
      throw new IllegalArgumentException("Number of threads must be greater than 0");
    }

    if (executorService == null) {
      throw new IllegalArgumentException("ExecutorService must not be null");
    }

    if (!enablePriority) {
      throw new IllegalStateException(
          "Priority must be enabled to add a worker pool with priority threshold");
    }

    this.workerPools.add(new WorkerPoolConfig(threads, priorityThreshold, executorService));
    return this;
  }

  public Scheduler build() {
    if (schedulerName == null) {
      schedulerName = new SchedulerName.Hostname();
    }

    final TaskResolver taskResolver =
        new TaskResolver(new SchedulerListeners(schedulerListeners), clock, knownTasks);
    final JdbcCustomization jdbcCustomization =
        ofNullable(this.jdbcCustomization)
            .orElseGet(
                () -> new AutodetectJdbcCustomization(dataSource, alwaysPersistTimestampInUTC));
    final JdbcTaskRepository schedulerTaskRepository =
        new JdbcTaskRepository(
            dataSource,
            true,
            jdbcCustomization,
            tableName,
            taskResolver,
            schedulerName,
            serializer,
            enablePriority,
            clock);
    final JdbcTaskRepository clientTaskRepository =
        new JdbcTaskRepository(
            dataSource,
            commitWhenAutocommitDisabled,
            jdbcCustomization,
            tableName,
            taskResolver,
            schedulerName,
            serializer,
            enablePriority,
            clock);

    ExecutorService candidateExecutorService = executorService;
    if (candidateExecutorService == null) {
      candidateExecutorService =
          Executors.newFixedThreadPool(
              executorThreads, defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-"));
    }

    ExecutorService candidateDueExecutor = dueExecutor;
    if (candidateDueExecutor == null) {
      candidateDueExecutor =
          Executors.newSingleThreadExecutor(
              defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-execute-due-"));
    }

    ScheduledExecutorService candidateHousekeeperExecutor = housekeeperExecutor;
    if (candidateHousekeeperExecutor == null) {
      candidateHousekeeperExecutor =
          Executors.newScheduledThreadPool(
              3, defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-housekeeper-"));
    }

    if (statsRegistry != null) {
      addSchedulerListener(new StatsRegistryAdapter(statsRegistry));
    }

    Waiter waiter = buildWaiter();

    double upperFraction = pollingStrategyConfig.upperLimitFractionOfThreads();
    double lowerFraction = pollingStrategyConfig.lowerLimitFractionOfThreads();

    // Create all executors
    List<Executor> executors = new ArrayList<>();

    // Default executor (accepts all priorities)
    int defaultUpperLimit = (int) (executorThreads * upperFraction);
    int defaultLowerLimit = (int) (executorThreads * lowerFraction);
    Executor defaultExecutor =
        new Executor(
            candidateExecutorService,
            clock,
            Integer.MIN_VALUE,
            defaultUpperLimit,
            defaultLowerLimit);
    executors.add(defaultExecutor);

    // Worker pool executors
    int totalThreads = executorThreads;
    for (WorkerPoolConfig workerPool : workerPools) {
      int poolThreads = getWorkerPoolThreads(workerPool);
      totalThreads += poolThreads;
      int poolUpperLimit = (int) (poolThreads * upperFraction);
      int poolLowerLimit = (int) (poolThreads * lowerFraction);
      Executor executor =
          new Executor(
              workerPool.executorService(),
              clock,
              workerPool.priorityThreshold(),
              poolUpperLimit,
              poolLowerLimit);
      executors.add(executor);
    }

    LOG.info(
        "Creating scheduler with configuration: threads={} (total across all pools: {}), pollInterval={}s, heartbeat={}s, enable-immediate-execution={}, enable-priority={}, table-name={}, name={}",
        executorThreads,
        totalThreads,
        waiter.getWaitDuration().getSeconds(),
        heartbeatInterval.getSeconds(),
        enableImmediateExecution,
        enablePriority,
        tableName,
        schedulerName.getName());

    final Scheduler scheduler =
        new Scheduler(
            clock,
            schedulerTaskRepository,
            clientTaskRepository,
            taskResolver,
            executors,
            schedulerName,
            waiter,
            heartbeatInterval,
            numberOfMissedHeartbeatsBeforeDead,
            schedulerListeners,
            executionInterceptors,
            pollingStrategyConfig,
            deleteUnresolvedAfter,
            shutdownMaxWait,
            logLevel,
            logStackTrace,
            startTasks,
            candidateDueExecutor,
            candidateHousekeeperExecutor);

    if (enableImmediateExecution) {
      scheduler.registerSchedulerListener(new ImmediateCheckForDueExecutions(scheduler, clock));
    }

    if (registerShutdownHook) {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    LOG.info("Received shutdown signal.");
                    scheduler.stop();
                  }));
    }

    return scheduler;
  }

  protected Waiter buildWaiter() {
    return new Waiter(poolingInterval, clock);
  }
}
