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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

import com.github.kagkarlsson.scheduler.SchedulerState.SettableSchedulerState;
import com.github.kagkarlsson.scheduler.logging.ConfigurableLogger;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scheduler implements SchedulerClient {

  public static final double TRIGGER_NEXT_BATCH_WHEN_AVAILABLE_THREADS_RATIO = 0.5;
  public static final String THREAD_PREFIX = "db-scheduler";
  private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
  private final SchedulerClient delegate;
  final Clock clock;
  final TaskRepository schedulerTaskRepository;
  final TaskResolver taskResolver;
  protected final PollStrategy executeDueStrategy;
  protected final Executor executor;
  private final ScheduledExecutorService housekeeperExecutor;
  private final HeartbeatConfig heartbeatConfig;
  private final int numberOfMissedHeartbeatsBeforeDead;
  int threadpoolSize;
  private final Waiter executeDueWaiter;
  private final Duration deleteUnresolvedAfter;
  private final Duration shutdownMaxWait;
  protected final List<OnStartup> onStartup;
  private final Waiter detectDeadWaiter;
  private final Duration heartbeatInterval;
  final StatsRegistry statsRegistry;
  private final ExecutorService dueExecutor;
  private final Waiter heartbeatWaiter;
  final SettableSchedulerState schedulerState = new SettableSchedulerState();
  final ConfigurableLogger failureLogger;
  private SchedulerClientEventListener earlyExecutionListener;

  protected Scheduler(
      Clock clock,
      TaskRepository schedulerTaskRepository,
      TaskRepository clientTaskRepository,
      TaskResolver taskResolver,
      int threadpoolSize,
      ExecutorService executorService,
      SchedulerName schedulerName,
      Waiter executeDueWaiter,
      Duration heartbeatInterval,
      int numberOfMissedHeartbeatsBeforeDead,
      boolean enableImmediateExecution,
      StatsRegistry statsRegistry,
      PollingStrategyConfig pollingStrategyConfig,
      Duration deleteUnresolvedAfter,
      Duration shutdownMaxWait,
      LogLevel logLevel,
      boolean logStackTrace,
      List<OnStartup> onStartup,
      ExecutorService dueExecutor,
      ScheduledExecutorService housekeeperExecutor) {
    this.clock = clock;
    this.schedulerTaskRepository = schedulerTaskRepository;
    this.taskResolver = taskResolver;
    this.threadpoolSize = threadpoolSize;
    this.executor = new Executor(executorService, clock);
    this.executeDueWaiter = executeDueWaiter;
    this.deleteUnresolvedAfter = deleteUnresolvedAfter;
    this.shutdownMaxWait = shutdownMaxWait;
    this.onStartup = onStartup;
    this.detectDeadWaiter = new Waiter(heartbeatInterval.multipliedBy(2), clock);
    this.heartbeatInterval = heartbeatInterval;
    this.numberOfMissedHeartbeatsBeforeDead = numberOfMissedHeartbeatsBeforeDead;
    this.heartbeatWaiter = new Waiter(heartbeatInterval, clock);
    this.heartbeatConfig =
        new HeartbeatConfig(
            heartbeatInterval, numberOfMissedHeartbeatsBeforeDead, getMaxAgeBeforeConsideredDead());
    this.statsRegistry = statsRegistry;
    this.dueExecutor = dueExecutor;
    this.housekeeperExecutor = housekeeperExecutor;
    earlyExecutionListener =
        (enableImmediateExecution
            ? new TriggerCheckForDueExecutions(schedulerState, clock, executeDueWaiter)
            : SchedulerClientEventListener.NOOP);
    delegate = new StandardSchedulerClient(clientTaskRepository, earlyExecutionListener, clock);
    this.failureLogger = ConfigurableLogger.create(LOG, logLevel, logStackTrace);

    if (pollingStrategyConfig.type == PollingStrategyConfig.Type.LOCK_AND_FETCH) {
      schedulerTaskRepository.verifySupportsLockAndFetch();
      executeDueStrategy =
          new LockAndFetchCandidates(
              executor,
              schedulerTaskRepository,
              this,
              earlyExecutionListener,
              threadpoolSize,
              statsRegistry,
              schedulerState,
              failureLogger,
              taskResolver,
              clock,
              pollingStrategyConfig,
              this::triggerCheckForDueExecutions,
              heartbeatConfig);
    } else if (pollingStrategyConfig.type == PollingStrategyConfig.Type.FETCH) {
      executeDueStrategy =
          new FetchCandidates(
              executor,
              schedulerTaskRepository,
              this,
              earlyExecutionListener,
              threadpoolSize,
              statsRegistry,
              schedulerState,
              failureLogger,
              taskResolver,
              clock,
              pollingStrategyConfig,
              this::triggerCheckForDueExecutions,
              heartbeatConfig);
    } else {
      throw new IllegalArgumentException(
          "Unknown polling-strategy type: " + pollingStrategyConfig.type);
    }
    LOG.info("Using polling-strategy: " + pollingStrategyConfig.describe());
  }

  public void start() {
    LOG.info("Starting scheduler.");

    executeOnStartup();

    dueExecutor.submit(
        new RunUntilShutdown(executeDueStrategy, executeDueWaiter, schedulerState, statsRegistry));

    housekeeperExecutor.scheduleWithFixedDelay(
        new RunAndLogErrors(this::detectDeadExecutions, statsRegistry),
        0,
        detectDeadWaiter.getWaitDuration().toMillis(),
        MILLISECONDS);
    housekeeperExecutor.scheduleWithFixedDelay(
        new RunAndLogErrors(this::updateHeartbeats, statsRegistry),
        0,
        heartbeatWaiter.getWaitDuration().toMillis(),
        MILLISECONDS);

    schedulerState.setStarted();
  }

  protected void executeDue() {
    this.executeDueStrategy.run();
  }

  protected void executeOnStartup() {
    // Client used for OnStartup always commits
    final StandardSchedulerClient onStartupClient =
        new StandardSchedulerClient(schedulerTaskRepository, clock);
    onStartup.forEach(
        os -> {
          try {
            os.onStartup(onStartupClient, this.clock);
          } catch (Exception e) {
            LOG.error("Unexpected error while executing OnStartup tasks. Continuing.", e);
            statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
          }
        });
  }

  public void stop() {
    stop(Duration.ofSeconds(1), Duration.ofSeconds(5));
  }

  void stop(Duration utilExecutorsWaitBeforeInterrupt, Duration utilExecutorsWaitAfterInterrupt) {
    if (schedulerState.isShuttingDown()) {
      LOG.warn("Multiple calls to 'stop()'. Scheduler is already stopping.");
      return;
    }

    schedulerState.setIsShuttingDown();
    LOG.info("Shutting down Scheduler.");

    if (executeDueWaiter.isWaiting()) {
      // Sleeping => interrupt
      dueExecutor.shutdownNow();
      if (!ExecutorUtils.awaitTermination(dueExecutor, utilExecutorsWaitAfterInterrupt)) {
        LOG.warn("Failed to shutdown due-executor properly.");
      }
    } else {
      // If currently running, i.e. checking for due, do not interrupt (try normal shutdown first)
      if (!ExecutorUtils.shutdownAndAwaitTermination(
          dueExecutor, utilExecutorsWaitBeforeInterrupt, utilExecutorsWaitAfterInterrupt)) {
        LOG.warn("Failed to shutdown due-executor properly.");
      }
    }

    executor.stop(shutdownMaxWait);

    // Shutdown heartbeating thread last
    if (!ExecutorUtils.shutdownAndAwaitTermination(
        housekeeperExecutor, utilExecutorsWaitBeforeInterrupt, utilExecutorsWaitAfterInterrupt)) {
      LOG.warn("Failed to shutdown housekeeper-executor properly.");
    }
  }

  public void pause() {
    LOG.info("Pausing scheduler.");
    this.schedulerState.setPaused(true);
  }

  public void resume() {
    LOG.info("Resuming scheduler.");
    this.schedulerState.setPaused(false);
  }

  public SchedulerState getSchedulerState() {
    return schedulerState;
  }

  @Override
  public <T> void schedule(SchedulableInstance<T> schedulableInstance) {
    this.delegate.schedule(schedulableInstance);
  }

  @Override
  public <T> void schedule(TaskInstance<T> taskInstance, Instant executionTime) {
    this.delegate.schedule(taskInstance, executionTime);
  }

  @Override
  public void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime) {
    this.delegate.reschedule(taskInstanceId, newExecutionTime);
  }

  @Override
  public <T> void reschedule(SchedulableInstance<T> schedulableInstance) {
    this.delegate.reschedule(schedulableInstance);
  }

  @Override
  public <T> void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime, T newData) {
    this.delegate.reschedule(taskInstanceId, newExecutionTime, newData);
  }

  @Override
  public void cancel(TaskInstanceId taskInstanceId) {
    this.delegate.cancel(taskInstanceId);
  }

  @Override
  public void fetchScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer) {
    this.delegate.fetchScheduledExecutions(consumer);
  }

  @Override
  public void fetchScheduledExecutions(
      ScheduledExecutionsFilter filter, Consumer<ScheduledExecution<Object>> consumer) {
    this.delegate.fetchScheduledExecutions(filter, consumer);
  }

  @Override
  public <T> void fetchScheduledExecutionsForTask(
      String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer) {
    this.delegate.fetchScheduledExecutionsForTask(taskName, dataClass, consumer);
  }

  @Override
  public <T> void fetchScheduledExecutionsForTask(
      String taskName,
      Class<T> dataClass,
      ScheduledExecutionsFilter filter,
      Consumer<ScheduledExecution<T>> consumer) {
    this.delegate.fetchScheduledExecutionsForTask(taskName, dataClass, filter, consumer);
  }

  @Override
  public Optional<ScheduledExecution<Object>> getScheduledExecution(TaskInstanceId taskInstanceId) {
    return this.delegate.getScheduledExecution(taskInstanceId);
  }

  public List<Execution> getFailingExecutions(Duration failingAtLeastFor) {
    return schedulerTaskRepository.getExecutionsFailingLongerThan(failingAtLeastFor);
  }

  public void triggerCheckForDueExecutions() {
    executeDueWaiter.wakeOrSkipNextWait();
  }

  public List<CurrentlyExecuting> getCurrentlyExecuting() {
    return executor.getCurrentlyExecuting();
  }

  public List<CurrentlyExecuting> getCurrentlyExecutingWithStaleHeartbeat() {
    return executor.getCurrentlyExecuting().stream()
        .filter(c -> c.getHeartbeatState().hasStaleHeartbeat())
        .collect(toList());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected void detectDeadExecutions() {
    LOG.debug("Deleting executions with unresolved tasks.");
    taskResolver
        .getUnresolvedTaskNames(deleteUnresolvedAfter)
        .forEach(
            taskName -> {
              LOG.warn(
                  "Deleting all executions for task with name '{}'. They have been unresolved for more than {}",
                  taskName,
                  deleteUnresolvedAfter);
              int removed = schedulerTaskRepository.removeExecutions(taskName);
              LOG.info("Removed {} executions", removed);
              taskResolver.clearUnresolved(taskName);
            });

    LOG.debug("Checking for dead executions.");
    Instant now = clock.now();
    final Instant oldAgeLimit = now.minus(getMaxAgeBeforeConsideredDead());
    List<Execution> oldExecutions = schedulerTaskRepository.getDeadExecutions(oldAgeLimit);

    if (!oldExecutions.isEmpty()) {
      oldExecutions.forEach(
          execution -> {
            LOG.info("Found dead execution. Delegating handling to task. Execution: " + execution);
            try {

              Optional<Task> task = taskResolver.resolve(execution.taskInstance.getTaskName());
              if (task.isPresent()) {
                statsRegistry.register(SchedulerStatsEvent.DEAD_EXECUTION);
                task.get()
                    .getDeadExecutionHandler()
                    .deadExecution(
                        ExecutionComplete.failure(execution, now, now, null),
                        new ExecutionOperations(
                            schedulerTaskRepository, earlyExecutionListener, execution));
              } else {
                LOG.error(
                    "Failed to find implementation for task with name '{}' for detected dead execution. Either delete the execution from the databaser, or add an implementation for it.",
                    execution.taskInstance.getTaskName());
              }

            } catch (Throwable e) {
              LOG.error(
                  "Failed while handling dead execution {}. Will be tried again later.",
                  execution,
                  e);
              statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
            }
          });
    } else {
      LOG.trace("No dead executions found.");
    }
    statsRegistry.register(SchedulerStatsEvent.RAN_DETECT_DEAD);
  }

  void updateHeartbeats() {
    final List<CurrentlyExecuting> currentlyProcessing = executor.getCurrentlyExecuting();
    if (currentlyProcessing.isEmpty()) {
      LOG.trace("No executions to update heartbeats for. Skipping.");
      return;
    }

    LOG.debug("Updating heartbeats for {} executions being processed.", currentlyProcessing.size());
    Instant now = clock.now();
    currentlyProcessing.forEach(execution -> updateHeartbeatForExecution(now, execution));
    statsRegistry.register(SchedulerStatsEvent.RAN_UPDATE_HEARTBEATS);
  }

  protected void updateHeartbeatForExecution(Instant now, CurrentlyExecuting currentlyExecuting) {
    // There is a race-condition: the execution may have been deleted or updated, causing
    // this update to fail (or update 0 rows). This may happen once, but not multiple times.

    Execution e = currentlyExecuting.getExecution();
    LOG.trace("Updating heartbeat for execution: " + e);

    try {
      boolean successfulHeartbeat = schedulerTaskRepository.updateHeartbeatWithRetry(e, now, 3);
      currentlyExecuting.heartbeat(successfulHeartbeat, now);

      if (!successfulHeartbeat) {
        statsRegistry.register(SchedulerStatsEvent.FAILED_HEARTBEAT);
      }

      HeartbeatState heartbeatState = currentlyExecuting.getHeartbeatState();
      if (heartbeatState.getFailedHeartbeats() > 1) {
        LOG.warn(
            "Execution has more than 1 failed heartbeats. Should not happen. Risk of being"
                + " considered dead. See heartbeat-state. Heartbeat-state={}, Execution={}",
            heartbeatState.describe(),
            e);
        statsRegistry.register(SchedulerStatsEvent.FAILED_MULTIPLE_HEARTBEATS);
      }

    } catch (Throwable ex) { // just-in-case to avoid any "poison-pills"
      LOG.error("Unexpteced failure while while updating heartbeat for execution {}.", e, ex);
      statsRegistry.register(SchedulerStatsEvent.FAILED_HEARTBEAT);
      statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
    }
  }

  Duration getMaxAgeBeforeConsideredDead() {
    return heartbeatInterval.multipliedBy(numberOfMissedHeartbeatsBeforeDead);
  }

  public static SchedulerBuilder create(DataSource dataSource, Task<?>... knownTasks) {
    return create(dataSource, Arrays.asList(knownTasks));
  }

  public static SchedulerBuilder create(DataSource dataSource, List<Task<?>> knownTasks) {
    return new SchedulerBuilder(dataSource, knownTasks);
  }
}
