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

import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

public class Scheduler {

	private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	public static final int SHUTDOWN_WAIT = 30;
	public static final TimeUnit SHUTDOWN_WAIT_TIMEUNIT = TimeUnit.MINUTES;
	private final Clock clock;
	private final TaskRepository taskRepository;
	private final ExecutorService executorService;
	private final Waiter waiter;
	private final Waiter detectDeadWaiter;
	private final Duration heartbeatInterval;
	private final StatsRegistry statsRegistry;
	private final ExecutorService dueExecutor;
	private final ExecutorService detectDeadExecutor;
	private final ExecutorService updateHeartbeatExecutor;
	private final Map<Execution, Execution> currentlyProcessing = Collections.synchronizedMap(new HashMap<>());
	private final Waiter heartbeatWaiter;
	final Semaphore executorsSemaphore;

	private boolean shutdownRequested = false;

	Scheduler(Clock clock, TaskRepository taskRepository, int maxThreads, SchedulerName schedulerName,
			  Waiter waiter, Duration updateHeartbeatWaiter, StatsRegistry statsRegistry) {
		this(clock, taskRepository, maxThreads, defaultExecutorService(maxThreads, schedulerName), schedulerName, waiter, updateHeartbeatWaiter, statsRegistry);
	}

	private static ExecutorService defaultExecutorService(int maxThreads, SchedulerName schedulerName) {
		return Executors.newFixedThreadPool(maxThreads, new ThreadFactoryBuilder().setNameFormat(schedulerName.getName() + "-%d").build());
	}

	Scheduler(Clock clock, TaskRepository taskRepository, int maxThreads, ExecutorService executorService, SchedulerName schedulerName,
			  Waiter waiter, Duration heartbeatInterval, StatsRegistry statsRegistry) {
		this.clock = clock;
		this.taskRepository = taskRepository;
		this.executorService = executorService;
		this.waiter = waiter;
		this.detectDeadWaiter = new Waiter(heartbeatInterval.multipliedBy(2));
		this.heartbeatInterval = heartbeatInterval;
		this.heartbeatWaiter = new Waiter(heartbeatInterval);
		this.statsRegistry = statsRegistry;
		this.dueExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(schedulerName.getName() + "-execute-due").build());
		this.detectDeadExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(schedulerName.getName() + "-detect-dead").build());
		this.updateHeartbeatExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(schedulerName.getName() + "-update-heartbeat").build());
		executorsSemaphore = new Semaphore(maxThreads);
	}

	public void start() {
		LOG.info("Starting scheduler");

		dueExecutor.submit(new RunUntilShutdown(this::executeDue, waiter));
		detectDeadExecutor.submit(new RunUntilShutdown(this::detectDeadExecutions, detectDeadWaiter));
		updateHeartbeatExecutor.submit(new RunUntilShutdown(this::updateHeartbeats, heartbeatWaiter));
	}

	public void stop() {
		shutdownRequested = true;
		LOG.info("Shutting down Scheduler. Will wait {} {} for running tasks to complete.", SHUTDOWN_WAIT, SHUTDOWN_WAIT_TIMEUNIT);
		MoreExecutors.shutdownAndAwaitTermination(dueExecutor, 5, TimeUnit.SECONDS);
		MoreExecutors.shutdownAndAwaitTermination(detectDeadExecutor, 5, TimeUnit.SECONDS);

		final boolean result = MoreExecutors.shutdownAndAwaitTermination(executorService, SHUTDOWN_WAIT, SHUTDOWN_WAIT_TIMEUNIT);

		if (result) {
			LOG.info("Scheduler stopped.");
		} else {
			LOG.warn("Scheduler stopped, but some tasks did not complete.");
		}
	}

	public void scheduleForExecution(LocalDateTime exeecutionTime, TaskInstance taskInstance) {
		taskRepository.createIfNotExists(new Execution(exeecutionTime, taskInstance));
	}

	void executeDue() {
		if (executorsSemaphore.availablePermits() <= 0) {
			return;
		}

		LocalDateTime now = clock.now();
		List<Execution> dueExecutions = taskRepository.getDue(now);

		int count = 0;
		LOG.trace("Found {} taskinstances due for execution", dueExecutions.size());
		for (Execution e : dueExecutions) {

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
				return;
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
					currentlyProcessing.put(pickedExecution.get(), pickedExecution.get());
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

	void detectDeadExecutions() {
		LOG.debug("Checking for dead executions.");
		LocalDateTime now = clock.now();
		final LocalDateTime oldAgeLimit = now.minus(getMaxAgeBeforeConsideredDead());
		List<Execution> oldExecutions = taskRepository.getOldExecutions(oldAgeLimit);

		if (!oldExecutions.isEmpty()) {
			oldExecutions.stream().forEach(execution -> {
				LOG.info("Found dead execution. Delegating handling to task. Execution: " + execution);
				try {
					execution.taskInstance.getTask().handleDeadExecution(execution, new ExecutionOperations(taskRepository, execution));
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
		LocalDateTime now = clock.now();
		new ArrayList<>(currentlyProcessing.values()).stream().forEach(execution -> {
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

	private class ExecuteTask implements Runnable {
		private final Execution execution;

		private ExecuteTask(Execution execution) {
			this.execution = execution;
		}

		@Override
		public void run() {
			try {
				final Task task = execution.taskInstance.getTask();
				LOG.debug("Executing " + execution);
				task.execute(execution.taskInstance);
				LOG.debug("Execution done");
				complete(execution, ExecutionComplete.Result.OK);

			} catch (RuntimeException unhandledException) {
				LOG.warn("Unhandled exception during execution. Treating as failure.", unhandledException);
				complete(execution, ExecutionComplete.Result.FAILED);

			} catch (Throwable unhandledError) {
				LOG.error("Error during execution. Treating as failure.", unhandledError);
				complete(execution, ExecutionComplete.Result.FAILED);
			}
		}

		private void complete(Execution execution, ExecutionComplete.Result result) {
			try {
				final Task task = execution.taskInstance.getTask();
				task.complete(new ExecutionComplete(execution, clock.now(), result), new ExecutionOperations(taskRepository, execution));
			} catch (Throwable e) {
				statsRegistry.registerUnexpectedError();
				LOG.error("Failed while completing execution {}. Execution will likely remain scheduled and locked/picked. " +
						"The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, getMaxAgeBeforeConsideredDead(), e);
			}
		}
	}

	private class RunUntilShutdown implements Runnable {
		private final Runnable toRun;
		private final Waiter waitBetweenRuns;

		public RunUntilShutdown(Runnable toRun, Waiter waitBetweenRuns) {
			this.toRun = toRun;
			this.waitBetweenRuns = waitBetweenRuns;
		}

		@Override
		public void run() {
			while (!shutdownRequested) {
				try {
					toRun.run();
					waitBetweenRuns.doWait();

				} catch (InterruptedException interruptedException) {
					if (shutdownRequested) {
						LOG.info("Thread '{}' interrupted due to shutdown.", Thread.currentThread().getName());
					} else {
						LOG.error("Unexpected interruption of thread. Will keep running.", interruptedException);
						statsRegistry.registerUnexpectedError();
					}
				} catch (Throwable e) {
					LOG.error("Unhandled exception. Will keep running.", e);
					statsRegistry.registerUnexpectedError();
				}
			}
		}
	}

	public static Builder create(DataSource dataSource, SchedulerName schedulerName, List<Task> knownTasks) {
		return new Builder(dataSource, schedulerName, knownTasks);
	}

	public static class Builder {

		private final DataSource dataSource;
		private final SchedulerName schedulerName;
		private int executorThreads = 10;
		private List<Task> knownTasks = new ArrayList<>();
		private Waiter waiter = new Waiter(Duration.ofSeconds(1));
		private StatsRegistry statsRegistry = StatsRegistry.NOOP;
		private Duration heartbeatInterval = Duration.ofMinutes(5);

		public Builder(DataSource dataSource, SchedulerName schedulerName, List<Task> knownTasks) {
			this.dataSource = dataSource;
			this.schedulerName = schedulerName;
			this.knownTasks = knownTasks;
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

		public Scheduler build() {
			final TaskResolver taskResolver = new TaskResolver(knownTasks, TaskResolver.OnCannotResolve.WARN_ON_UNRESOLVED);
			final JdbcTaskRepository taskRepository = new JdbcTaskRepository(dataSource, taskResolver, schedulerName);

			return new Scheduler(new SystemClock(), taskRepository, executorThreads, schedulerName, waiter, heartbeatInterval, statsRegistry);
		}
	}


	public static class NoAvailableExecutors extends RuntimeException {
	}
}
