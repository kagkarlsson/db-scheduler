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

import com.github.kagkarlsson.scheduler.SchedulerState.SettableSchedulerState;
import com.github.kagkarlsson.scheduler.concurrent.LoggingRunnable;
import com.github.kagkarlsson.scheduler.logging.ConfigurableLogger;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import com.github.kagkarlsson.scheduler.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.github.kagkarlsson.scheduler.ExecutorUtils.defaultThreadFactoryWithPrefix;

public class Scheduler implements SchedulerClient {

    public static final double TRIGGER_NEXT_BATCH_WHEN_AVAILABLE_THREADS_RATIO = 0.5;
    public static final String THREAD_PREFIX = "db-scheduler";
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    private final SchedulerClient delegate;
    private final Clock clock;
    private final TaskRepository schedulerTaskRepository;
    private final TaskResolver taskResolver;
    private int threadpoolSize;
    private final ExecutorService executorService;
    private final Waiter executeDueWaiter;
    private final Duration deleteUnresolvedAfter;
    private final Duration shutdownMaxWait;
    protected final List<OnStartup> onStartup;
    private final Waiter detectDeadWaiter;
    private final Duration heartbeatInterval;
    private final StatsRegistry statsRegistry;
    private final int pollingLimit;
    private final ExecutorService dueExecutor;
    private final ExecutorService detectDeadExecutor;
    private final ExecutorService updateHeartbeatExecutor;
    private final Map<Execution, CurrentlyExecuting> currentlyProcessing = Collections.synchronizedMap(new HashMap<>());
    private final Waiter heartbeatWaiter;
    private final SettableSchedulerState schedulerState = new SettableSchedulerState();
    private int currentGenerationNumber = 1;
    private final ConfigurableLogger failureLogger;

    protected Scheduler(Clock clock, TaskRepository schedulerTaskRepository, TaskRepository clientTaskRepository, TaskResolver taskResolver, int threadpoolSize, ExecutorService executorService, SchedulerName schedulerName,
                        Waiter executeDueWaiter, Duration heartbeatInterval, boolean enableImmediateExecution, StatsRegistry statsRegistry, int pollingLimit, Duration deleteUnresolvedAfter, Duration shutdownMaxWait,
                        LogLevel logLevel, boolean logStackTrace, List<OnStartup> onStartup) {
        this.clock = clock;
        this.schedulerTaskRepository = schedulerTaskRepository;
        this.taskResolver = taskResolver;
        this.threadpoolSize = threadpoolSize;
        this.executorService = executorService;
        this.executeDueWaiter = executeDueWaiter;
        this.deleteUnresolvedAfter = deleteUnresolvedAfter;
        this.shutdownMaxWait = shutdownMaxWait;
        this.onStartup = onStartup;
        this.detectDeadWaiter = new Waiter(heartbeatInterval.multipliedBy(2), clock);
        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatWaiter = new Waiter(heartbeatInterval, clock);
        this.statsRegistry = statsRegistry;
        this.pollingLimit = pollingLimit;
        this.dueExecutor = Executors.newSingleThreadExecutor(defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-execute-due-"));
        this.detectDeadExecutor = Executors.newSingleThreadExecutor(defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-detect-dead-"));
        this.updateHeartbeatExecutor = Executors.newSingleThreadExecutor(defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-update-heartbeat-"));
        SchedulerClientEventListener earlyExecutionListener = (enableImmediateExecution ? new TriggerCheckForDueExecutions(schedulerState, clock, executeDueWaiter) : SchedulerClientEventListener.NOOP);
        delegate = new StandardSchedulerClient(clientTaskRepository, earlyExecutionListener);
        this.failureLogger = ConfigurableLogger.create(LOG, logLevel, logStackTrace);
    }

    public void start() {
        LOG.info("Starting scheduler.");

        executeOnStartup();

        dueExecutor.submit(new RunUntilShutdown(this::executeDue, executeDueWaiter, schedulerState, statsRegistry));
        detectDeadExecutor.submit(new RunUntilShutdown(this::detectDeadExecutions, detectDeadWaiter, schedulerState, statsRegistry));
        updateHeartbeatExecutor.submit(new RunUntilShutdown(this::updateHeartbeats, heartbeatWaiter, schedulerState, statsRegistry));

        schedulerState.setStarted();
    }

    protected void executeOnStartup() {
        onStartup.forEach(os -> {
            try {
                os.onStartup(this, this.clock);
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
        if (!ExecutorUtils.shutdownAndAwaitTermination(dueExecutor, utilExecutorsWaitBeforeInterrupt, utilExecutorsWaitAfterInterrupt)) {
            LOG.warn("Failed to shutdown due-executor properly.");
        }
        if (!ExecutorUtils.shutdownAndAwaitTermination(detectDeadExecutor, utilExecutorsWaitBeforeInterrupt, utilExecutorsWaitAfterInterrupt)) {
            LOG.warn("Failed to shutdown detect-dead-executor properly.");
        }
        if (!ExecutorUtils.shutdownAndAwaitTermination(updateHeartbeatExecutor, utilExecutorsWaitBeforeInterrupt, utilExecutorsWaitAfterInterrupt)) {
            LOG.warn("Failed to shutdown update-heartbeat-executor properly.");
        }

        LOG.info("Letting running executions finish. Will wait up to 2x{}.", shutdownMaxWait);
        final Instant startShutdown = clock.now();
        if (ExecutorUtils.shutdownAndAwaitTermination(executorService, shutdownMaxWait, shutdownMaxWait)) {
            LOG.info("Scheduler stopped.");
        } else {
            LOG.warn("Scheduler stopped, but some tasks did not complete. Was currently running the following executions:\n{}",
                    new ArrayList<>(currentlyProcessing.keySet()).stream().map(Execution::toString).collect(Collectors.joining("\n")));
        }

        final Duration shutdownTime = Duration.between(startShutdown, clock.now());
        if (shutdownMaxWait.toMillis() > Duration.ofMinutes(1).toMillis()
            && shutdownTime.toMillis() >= shutdownMaxWait.toMillis()) {
            LOG.info("Shutdown of the scheduler executor service took {}. Consider regularly checking for " +
                "'executionContext.getSchedulerState().isShuttingDown()' in task execution-handler and abort when " +
                "scheduler is shutting down.", shutdownTime);
        }
    }

    public SchedulerState getSchedulerState() {
        return schedulerState;
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
    public void fetchScheduledExecutions(ScheduledExecutionsFilter filter, Consumer<ScheduledExecution<Object>> consumer) {
        this.delegate.fetchScheduledExecutions(filter, consumer);
    }

    @Override
    public <T> void fetchScheduledExecutionsForTask(String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer) {
        this.delegate.fetchScheduledExecutionsForTask(taskName, dataClass, consumer);
    }

    @Override
    public <T> void fetchScheduledExecutionsForTask(String taskName, Class<T> dataClass, ScheduledExecutionsFilter filter, Consumer<ScheduledExecution<T>> consumer) {
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
        return new ArrayList<>(currentlyProcessing.values());
    }

    protected void executeDue() {
        Instant now = clock.now();
        List<Execution> dueExecutions = schedulerTaskRepository.getDue(now, pollingLimit);
        LOG.trace("Found {} task instances due for execution", dueExecutions.size());

        this.currentGenerationNumber = this.currentGenerationNumber + 1;
        DueExecutionsBatch newDueBatch = new DueExecutionsBatch(Scheduler.this.threadpoolSize, currentGenerationNumber, dueExecutions.size(), pollingLimit == dueExecutions.size());

        for (Execution e : dueExecutions) {
            executorService.execute(new PickAndExecute(e, newDueBatch));
        }
        statsRegistry.register(SchedulerStatsEvent.RAN_EXECUTE_DUE);
    }

    @SuppressWarnings({"rawtypes","unchecked"})
    protected void detectDeadExecutions() {
        LOG.debug("Deleting executions with unresolved tasks.");
        taskResolver.getUnresolvedTaskNames(deleteUnresolvedAfter)
            .forEach(taskName -> {
                LOG.warn("Deleting all executions for task with name '{}'. They have been unresolved for more than {}", taskName, deleteUnresolvedAfter);
                int removed = schedulerTaskRepository.removeExecutions(taskName);
                LOG.info("Removed {} executions", removed);
                taskResolver.clearUnresolved(taskName);
            });

        LOG.debug("Checking for dead executions.");
        Instant now = clock.now();
        final Instant oldAgeLimit = now.minus(getMaxAgeBeforeConsideredDead());
        List<Execution> oldExecutions = schedulerTaskRepository.getDeadExecutions(oldAgeLimit);

        if (!oldExecutions.isEmpty()) {
            oldExecutions.forEach(execution -> {

                LOG.info("Found dead execution. Delegating handling to task. Execution: " + execution);
                try {

                    Optional<Task> task = taskResolver.resolve(execution.taskInstance.getTaskName());
                    if (task.isPresent()) {
                        statsRegistry.register(SchedulerStatsEvent.DEAD_EXECUTION);
                        task.get().getDeadExecutionHandler().deadExecution(execution, new ExecutionOperations(schedulerTaskRepository, execution));
                    } else {
                        LOG.error("Failed to find implementation for task with name '{}' for detected dead execution. Either delete the execution from the databaser, or add an implementation for it.", execution.taskInstance.getTaskName());
                    }

                } catch (Throwable e) {
                    LOG.error("Failed while handling dead execution {}. Will be tried again later.", execution, e);
                    statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                }
            });
        } else {
            LOG.trace("No dead executions found.");
        }
        statsRegistry.register(SchedulerStatsEvent.RAN_DETECT_DEAD);
    }

    void updateHeartbeats() {
        if (currentlyProcessing.isEmpty()) {
            LOG.trace("No executions to update heartbeats for. Skipping.");
            return;
        }

        LOG.debug("Updating heartbeats for {} executions being processed.", currentlyProcessing.size());
        Instant now = clock.now();
        new ArrayList<>(currentlyProcessing.keySet()).forEach(execution -> {
            LOG.trace("Updating heartbeat for execution: " + execution);
            try {
                schedulerTaskRepository.updateHeartbeat(execution, now);
            } catch (Throwable e) {
                LOG.error("Failed while updating heartbeat for execution {}. Will try again later.", execution, e);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
            }
        });
        statsRegistry.register(SchedulerStatsEvent.RAN_UPDATE_HEARTBEATS);
    }

    private Duration getMaxAgeBeforeConsideredDead() {
        return heartbeatInterval.multipliedBy(4);
    }

    private class PickAndExecute extends LoggingRunnable {
        private Execution candidate;
        private DueExecutionsBatch addedDueExecutionsBatch;

        public PickAndExecute(Execution candidate, DueExecutionsBatch dueExecutionsBatch) {
            this.candidate = candidate;
            this.addedDueExecutionsBatch = dueExecutionsBatch;
        }

        @Override
        public void runButLogExceptions() {
            if (schedulerState.isShuttingDown()) {
                LOG.info("Scheduler has been shutdown. Skipping fetched due execution: " + candidate.taskInstance.getTaskAndInstance());
                return;
            }

            try {
                if (addedDueExecutionsBatch.isOlderGenerationThan(currentGenerationNumber)) {
                    // skipping execution due to it being stale
                    addedDueExecutionsBatch.markBatchAsStale();
                    statsRegistry.register(StatsRegistry.CandidateStatsEvent.STALE);
                    LOG.trace("Skipping queued execution (current generationNumber: {}, execution generationNumber: {})", currentGenerationNumber, addedDueExecutionsBatch.getGenerationNumber());
                    return;
                }

                final Optional<Execution> pickedExecution = schedulerTaskRepository.pick(candidate, clock.now());

                if (!pickedExecution.isPresent()) {
                    // someone else picked id
                    LOG.debug("Execution picked by another scheduler. Continuing to next due execution.");
                    statsRegistry.register(StatsRegistry.CandidateStatsEvent.ALREADY_PICKED);
                    return;
                }

                currentlyProcessing.put(pickedExecution.get(), new CurrentlyExecuting(pickedExecution.get(), clock));
                try {
                    statsRegistry.register(StatsRegistry.CandidateStatsEvent.EXECUTED);
                    executePickedExecution(pickedExecution.get());
                } finally {
                    if (currentlyProcessing.remove(pickedExecution.get()) == null) {
                        // May happen in rare circumstances (typically concurrency tests)
                        LOG.warn("Released execution was not found in collection of executions currently being processed. Should never happen.");
                    }
                }
            } finally {
                // Make sure 'executionsLeftInBatch' is decremented for all executions (run or not run)
                addedDueExecutionsBatch.oneExecutionDone(Scheduler.this::triggerCheckForDueExecutions);
            }
        }

        private void executePickedExecution(Execution execution) {
            final Optional<Task> task = taskResolver.resolve(execution.taskInstance.getTaskName());
            if (!task.isPresent()) {
                LOG.error("Failed to find implementation for task with name '{}'. Should have been excluded in JdbcRepository.", execution.taskInstance.getTaskName());
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                return;
            }

            Instant executionStarted = clock.now();
            try {
                LOG.debug("Executing " + execution);
                CompletionHandler completion = task.get().execute(execution.taskInstance, new ExecutionContext(schedulerState, execution, Scheduler.this));
                LOG.debug("Execution done");

                complete(completion, execution, executionStarted);
                statsRegistry.register(StatsRegistry.ExecutionStatsEvent.COMPLETED);

            } catch (RuntimeException unhandledException) {
                failure(task.get(), execution, unhandledException, executionStarted, "Unhandled exception");
                statsRegistry.register(StatsRegistry.ExecutionStatsEvent.FAILED);

            } catch (Throwable unhandledError) {
                failure(task.get(), execution, unhandledError, executionStarted, "Error");
                statsRegistry.register(StatsRegistry.ExecutionStatsEvent.FAILED);
            }
        }

        private void complete(CompletionHandler completion, Execution execution, Instant executionStarted) {
            ExecutionComplete completeEvent = ExecutionComplete.success(execution, executionStarted, clock.now());
            try {
                completion.complete(completeEvent, new ExecutionOperations(schedulerTaskRepository, execution));
                statsRegistry.registerSingleCompletedExecution(completeEvent);
            } catch (Throwable e) {
                statsRegistry.register(SchedulerStatsEvent.COMPLETIONHANDLER_ERROR);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                LOG.error("Failed while completing execution {}. Execution will likely remain scheduled and locked/picked. " +
                        "The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, getMaxAgeBeforeConsideredDead(), e);
            }
        }

        private void failure(Task task, Execution execution, Throwable cause, Instant executionStarted, String errorMessagePrefix) {
            String logMessage = errorMessagePrefix + " during execution of task with name '{}'. Treating as failure.";
            failureLogger.log(logMessage, cause, task.getName());

            ExecutionComplete completeEvent = ExecutionComplete.failure(execution, executionStarted, clock.now(), cause);
            try {
                task.getFailureHandler().onFailure(completeEvent, new ExecutionOperations(schedulerTaskRepository, execution));
                statsRegistry.registerSingleCompletedExecution(completeEvent);
            } catch (Throwable e) {
                statsRegistry.register(SchedulerStatsEvent.FAILUREHANDLER_ERROR);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                LOG.error("Failed while completing execution {}. Execution will likely remain scheduled and locked/picked. " +
                        "The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, getMaxAgeBeforeConsideredDead(), e);
            }
        }

    }

    public static SchedulerBuilder create(DataSource dataSource, Task<?> ... knownTasks) {
        return create(dataSource, Arrays.asList(knownTasks));
    }

    public static SchedulerBuilder create(DataSource dataSource, List<Task<?>> knownTasks) {
        return new SchedulerBuilder(dataSource, knownTasks);
    }

}
