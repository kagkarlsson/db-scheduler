/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.SchedulerState.SettableSchedulerState;
import com.github.kagkarlsson.scheduler.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.github.kagkarlsson.scheduler.ExecutorUtils.defaultThreadFactoryWithPrefix;

public class Scheduler implements SchedulerClient {

	private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	private static final String THREAD_PREFIX = "db-scheduler";
	public static final Duration SHUTDOWN_WAIT = Duration.ofMinutes(30);
	private final SchedulerClient delegate;
	private final Clock clock;
	private final TaskRepository taskRepository;
	private final TaskResolver taskResolver;
	private final ExecutorService executorService;
	private final Waiter waiter;
	private final List<OnStartup> onStartup;
	private final Waiter detectDeadWaiter;
	private final Duration heartbeatInterval;
	private final StatsRegistry statsRegistry;
	private final ExecutorService dueExecutor;
	private final ExecutorService detectDeadExecutor;
	private final ExecutorService updateHeartbeatExecutor;
	private final Map<Execution, CurrentlyExecuting> currentlyProcessing = Collections.synchronizedMap(new HashMap<>());
	private final Waiter heartbeatWaiter;
	private final SettableSchedulerState schedulerState = new SettableSchedulerState();
	final Semaphore executorsSemaphore;

	Scheduler(Clock clock, TaskRepository taskRepository, TaskResolver taskResolver, int maxThreads, SchedulerName schedulerName,
			  Waiter waiter, Duration updateHeartbeatWaiter, StatsRegistry statsRegistry, List<OnStartup> onStartup) {
		this(clock, taskRepository, taskResolver, maxThreads, defaultExecutorService(maxThreads), schedulerName, waiter, updateHeartbeatWaiter, statsRegistry, onStartup);
	}

	private static ExecutorService defaultExecutorService(int maxThreads) {
		return Executors.newFixedThreadPool(maxThreads, defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-"));
	}

	Scheduler(Clock clock, TaskRepository taskRepository, TaskResolver taskResolver, int maxThreads, ExecutorService executorService, SchedulerName schedulerName,
			  Waiter waiter, Duration heartbeatInterval, StatsRegistry statsRegistry, List<OnStartup> onStartup) {
		this.clock = clock;
		this.taskRepository = taskRepository;
		this.taskResolver = taskResolver;
		this.executorService = executorService;
		this.waiter = waiter;
		this.onStartup = onStartup;
		this.detectDeadWaiter = new Waiter(heartbeatInterval.multipliedBy(2));
		this.heartbeatInterval = heartbeatInterval;
		this.heartbeatWaiter = new Waiter(heartbeatInterval);
		this.statsRegistry = statsRegistry;
		this.dueExecutor = Executors.newSingleThreadExecutor(defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-execute-due-"));
		this.detectDeadExecutor = Executors.newSingleThreadExecutor(defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-detect-dead-"));
		this.updateHeartbeatExecutor = Executors.newSingleThreadExecutor(defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-update-heartbeat-"));
		executorsSemaphore = new Semaphore(maxThreads);
		delegate = new StandardSchedulerClient(taskRepository);
	}

	public void start() {
		LOG.info("Starting scheduler");

		executeOnStartup(onStartup);

		dueExecutor.submit(new RunUntilShutdown(this::executeDue, waiter, schedulerState, statsRegistry));
		detectDeadExecutor.submit(new RunUntilShutdown(this::detectDeadExecutions, detectDeadWaiter, schedulerState, statsRegistry));
		updateHeartbeatExecutor.submit(new RunUntilShutdown(this::updateHeartbeats, heartbeatWaiter, schedulerState, statsRegistry));
	}

	private void executeOnStartup(List<OnStartup> onStartup) {
		onStartup.forEach(os -> {
			try {
				os.onStartup(this);
			} catch (Exception e) {
				LOG.error("Unexpected error while executing OnStartup tasks. Continuing.", e);
				statsRegistry.registerUnexpectedError();
			}
		});
	}

	public void stop() {
		if (schedulerState.isShuttingDown()) {
			LOG.warn("Multiple calls to 'stop()'. Scheduler is already stopping.");
			return;
		}

		schedulerState.setIsShuttingDown();

		LOG.info("Shutting down Scheduler.");
		if (!ExecutorUtils.shutdownNowAndAwaitTermination(dueExecutor, Duration.ofSeconds(5))) {
			LOG.warn("Failed to shutdown due-executor properly.");
		}
		if (!ExecutorUtils.shutdownNowAndAwaitTermination(detectDeadExecutor, Duration.ofSeconds(5))) {
			LOG.warn("Failed to shutdown detect-dead-executor properly.");
		}
		if (!ExecutorUtils.shutdownNowAndAwaitTermination(updateHeartbeatExecutor, Duration.ofSeconds(5))) {
			LOG.warn("Failed to shutdown update-heartbeat-executor properly.");
		}

		LOG.info("Letting running executions finish. Will wait up to {}.", SHUTDOWN_WAIT);
		if (ExecutorUtils.shutdownAndAwaitTermination(executorService, SHUTDOWN_WAIT)) {
			LOG.info("Scheduler stopped.");
		} else {
			LOG.warn("Scheduler stopped, but some tasks did not complete. Was currently running the following executions:\n{}",
					new ArrayList<>(currentlyProcessing.keySet()).stream().map(Execution::toString).collect(Collectors.joining("\n")));
		}
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
	public void cancel(TaskInstanceId taskInstanceId) {
		this.delegate.cancel(taskInstanceId);
	}

	@Override
	public <T> void getScheduled(Consumer<ScheduledExecution<T>> consumer) {
		this.delegate.getScheduled(consumer);
	}

	@Override
	public <T> void getScheduled(String taskName, Consumer<ScheduledExecution<T>> consumer) {
		this.delegate.getScheduled(taskName, consumer);
	}

	public List<CurrentlyExecuting> getCurrentlyExecuting() {
		return new ArrayList<>(currentlyProcessing.values());
	}

	public List<Execution> getFailingExecutions(Duration failingAtLeastFor) {
		return taskRepository.getExecutionsFailingLongerThan(failingAtLeastFor);
	}

	void executeDue() {
		if (executorsSemaphore.availablePermits() <= 0) {
			return;
		}

		Instant now = clock.now();
		List<Execution> dueExecutions = taskRepository.getDue(now);

		int count = 0;
		LOG.trace("Found {} taskinstances due for execution", dueExecutions.size());
		for (Execution e : dueExecutions) {
			if (schedulerState.isShuttingDown()) {
				LOG.info("Scheduler has been shutdown. Skipping {} due executions.", dueExecutions.size() - count);
				return;
			}

			final Optional<Execution> pickedExecution;
			try {
				pickedExecution = aquireExecutorAndPickExecution(e);
			} catch (NoAvailableExecutors ex) {
				LOG.debug("No available executors. Skipping {} due executions.", dueExecutions.size() - count);
				return;
			}

			if (pickedExecution.isPresent()) {
				CompletableFuture
						.runAsync(new ExecuteTask(pickedExecution.get()), executorService)
						.thenRun(() -> releaseExecutor(pickedExecution.get()));
			} else {
				LOG.debug("Execution picked by another scheduler. Continuing to next due execution.");
			}
			count++;
		}
	}

	private Optional<Execution> aquireExecutorAndPickExecution(Execution execution) {
		if (executorsSemaphore.tryAcquire()) {
			try {
				final Optional<Execution> pickedExecution = taskRepository.pick(execution, clock.now());

				if (!pickedExecution.isPresent()) {
					executorsSemaphore.release();
				} else {
					currentlyProcessing.put(pickedExecution.get(), new CurrentlyExecuting(pickedExecution.get(), clock));
				}
				return pickedExecution;

			} catch (Throwable t) {
				executorsSemaphore.release();
				throw t;
			}
		} else {
			throw new NoAvailableExecutors();
		}
	}

	private void releaseExecutor(Execution execution) {
		executorsSemaphore.release();
		if (currentlyProcessing.remove(execution) == null) {
			LOG.error("Released execution was not found in collection of executions currently being processed. Should never happen.");
			statsRegistry.registerUnexpectedError();
		}
	}

	@SuppressWarnings({"rawtypes","unchecked"})
	void detectDeadExecutions() {
		LOG.debug("Checking for dead executions.");
		Instant now = clock.now();
		final Instant oldAgeLimit = now.minus(getMaxAgeBeforeConsideredDead());
		List<Execution> oldExecutions = taskRepository.getOldExecutions(oldAgeLimit);

		if (!oldExecutions.isEmpty()) {
			oldExecutions.forEach(execution -> {

				LOG.info("Found dead execution. Delegating handling to task. Execution: " + execution);
				try {

					Optional<Task> task = taskResolver.resolve(execution.taskInstance.getTaskName());
					if (task.isPresent()) {
						task.get().getDeadExecutionHandler().deadExecution(execution, new ExecutionOperations(taskRepository, execution));
					} else {
						LOG.error("Failed to find implementation for task with name '{}' for detected dead execution. Either delete the execution from the databaser, or add an implementation for it.", execution.taskInstance.getTaskName());
					}

				} catch (Throwable e) {
					LOG.error("Failed while handling dead execution {}. Will be tried again later.", execution, e);
					statsRegistry.registerUnexpectedError();
				}
			});
		} else {
			LOG.trace("No dead executions found.");
		}
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
				taskRepository.updateHeartbeat(execution, now);
			} catch (Throwable e) {
				LOG.error("Failed while updating heartbeat for execution {}. Will try again later.", execution, e);
				statsRegistry.registerUnexpectedError();
			}
		});
	}

	private Duration getMaxAgeBeforeConsideredDead() {
		return heartbeatInterval.multipliedBy(4);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private class ExecuteTask implements Runnable {
		private final Execution execution;

		private ExecuteTask(Execution execution) {
			this.execution = execution;
		}

		@Override
		public void run() {
			final Optional<Task> task = taskResolver.resolve(execution.taskInstance.getTaskName());
			if (!task.isPresent()) {
				LOG.error("Failed to find implementation for task with name '{}'. If there are a high number of executions, this may block other executions and must be fixed.", execution.taskInstance.getTaskName());
				return;
			}

			try {
				LOG.debug("Executing " + execution);
				CompletionHandler completion = task.get().execute(execution.taskInstance, new ExecutionContext(schedulerState, execution, Scheduler.this));
				LOG.debug("Execution done");
				complete(completion, execution);

			} catch (RuntimeException unhandledException) {
				LOG.warn("Unhandled exception during execution. Treating as failure.", unhandledException);
				failure(task.get().getFailureHandler(), execution, unhandledException);

			} catch (Throwable unhandledError) {
				LOG.error("Error during execution. Treating as failure.", unhandledError);
				failure(task.get().getFailureHandler(), execution, unhandledError);
			}
		}

		private void complete(CompletionHandler completion, Execution execution) {
			try {
				completion.complete(ExecutionComplete.success(execution, clock.now()), new ExecutionOperations(taskRepository, execution));
			} catch (Throwable e) {
				statsRegistry.registerUnexpectedError();
				LOG.error("Failed while completing execution {}. Execution will likely remain scheduled and locked/picked. " +
						"The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, getMaxAgeBeforeConsideredDead(), e);
			}
		}

		private void failure(FailureHandler failureHandler, Execution execution, Throwable cause) {
			try {
				failureHandler.onFailure(ExecutionComplete.failure(execution, clock.now(), cause), new ExecutionOperations(taskRepository, execution));
			} catch (Throwable e) {
				statsRegistry.registerUnexpectedError();
				LOG.error("Failed while completing execution {}. Execution will likely remain scheduled and locked/picked. " +
						"The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, getMaxAgeBeforeConsideredDead(), e);
			}
		}
	}

	static class RunUntilShutdown implements Runnable {
		private final Runnable toRun;
		private final Waiter waitBetweenRuns;
		private final SchedulerState schedulerState;
		private final StatsRegistry statsRegistry;

		public RunUntilShutdown(Runnable toRun, Waiter waitBetweenRuns, SchedulerState schedulerState, StatsRegistry statsRegistry) {
			this.toRun = toRun;
			this.waitBetweenRuns = waitBetweenRuns;
			this.schedulerState = schedulerState;
			this.statsRegistry = statsRegistry;
		}

		@Override
		public void run() {
			while (!schedulerState.isShuttingDown()) {
				try {
					toRun.run();
				} catch (Throwable e) {
					LOG.error("Unhandled exception. Will keep running.", e);
					statsRegistry.registerUnexpectedError();
				}

				try {
					waitBetweenRuns.doWait();
				} catch (InterruptedException interruptedException) {
					if (schedulerState.isShuttingDown()) {
						LOG.debug("Thread '{}' interrupted due to shutdown.", Thread.currentThread().getName());
					} else {
						LOG.error("Unexpected interruption of thread. Will keep running.", interruptedException);
						statsRegistry.registerUnexpectedError();
					}
				}
			}
		}
	}

	public static Builder create(DataSource dataSource, Task<?> ... knownTasks) {
		return create(dataSource, Arrays.asList(knownTasks));
	}

	public static Builder create(DataSource dataSource, List<Task<?>> knownTasks) {
		return new Builder(dataSource, knownTasks);
	}

	public static class Builder {

		private final DataSource dataSource;
		private SchedulerName schedulerName = new SchedulerName.Hostname();
		private int executorThreads = 10;
		private final List<Task<?>> knownTasks = new ArrayList<>();
		private final List<OnStartup> startTasks = new ArrayList<>();
		private Waiter waiter = new Waiter(Duration.ofSeconds(10));
		private StatsRegistry statsRegistry = StatsRegistry.NOOP;
		private Duration heartbeatInterval = Duration.ofMinutes(5);
		private final Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;

		public Builder(DataSource dataSource, List<Task<?>> knownTasks) {
			this.dataSource = dataSource;
			this.knownTasks.addAll(knownTasks);
		}

		@SafeVarargs
		public final <T extends Task<?> & OnStartup> Builder startTasks(T... startTasks) {
			return startTasks(Arrays.asList(startTasks));
		}

		public <T extends Task<?> & OnStartup> Builder startTasks(List<T> startTasks) {
			knownTasks.addAll(startTasks);
			this.startTasks.addAll(startTasks);
			return this;
		}

		public Builder pollingInterval(Duration pollingInterval) {
			waiter = new Waiter(pollingInterval);
			return this;
		}

		public Builder heartbeatInterval(Duration duration) {
			this.heartbeatInterval = duration;
			return this;
		}

		public Builder threads(int numberOfThreads) {
			this.executorThreads = numberOfThreads;
			return this;
		}

		public Builder statsRegistry(StatsRegistry statsRegistry) {
			this.statsRegistry = statsRegistry;
			return this;
		}

		public Builder schedulerName(SchedulerName schedulerName) {
			this.schedulerName = schedulerName;
			return this;
		}

		public Scheduler build() {
			final TaskResolver taskResolver = new TaskResolver(knownTasks);
			final JdbcTaskRepository taskRepository = new JdbcTaskRepository(dataSource, taskResolver, schedulerName, serializer);

			return new Scheduler(new SystemClock(), taskRepository, taskResolver, executorThreads, schedulerName, waiter, heartbeatInterval, statsRegistry, startTasks);
		}
	}


	public static class NoAvailableExecutors extends RuntimeException {
	}
}
