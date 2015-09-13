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
	private static final Duration OLD_AGE = Duration.ofHours(6);
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
	private final Map<Execution,Execution> currentlyProcessing = Collections.synchronizedMap(new HashMap<>());
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
		this.detectDeadWaiter = new Waiter(heartbeatInterval.toMillis() * 2);
		this.heartbeatInterval = heartbeatInterval;
		this.heartbeatWaiter = new Waiter(heartbeatInterval.toMillis());
		this.statsRegistry = statsRegistry;
		this.dueExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(schedulerName.getName() + "-execute-due").build());
		this.detectDeadExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(schedulerName.getName() + "-detect-dead").build());
		this.updateHeartbeatExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(schedulerName.getName() + "-update-heartbeat").build());
		executorsSemaphore = new Semaphore(maxThreads);
	}

	public void start() {
		LOG.info("Starting scheduler");

		dueExecutor.submit(() -> {
			while (!shutdownRequested) {
				try {
					if (executorsSemaphore.availablePermits() > 0) {
						executeDue();
					}

					waiter.doWait();

				} catch (Throwable e) {
					statsRegistry.registerUnexpectedError();
					LOG.error("Unhandled exception", e);
				}
			}
		});

		detectDeadExecutor.submit(() -> {
			while (!shutdownRequested) {
				try {
					detectDeadExecutions();
					detectDeadWaiter.doWait();

				} catch (Throwable e) {
					statsRegistry.registerUnexpectedError();
					LOG.error("Unhandled exception", e);
				}
			}
		});

		updateHeartbeatExecutor.submit(() -> {
			while (!shutdownRequested) {
				try {
					updateHeartbeats();
					heartbeatWaiter.doWait();

				} catch (Throwable e) {
					statsRegistry.registerUnexpectedError();
					LOG.error("Unhandled exception", e);
				}
			}
		});
	}

	public void stop() {
		shutdownRequested = true;
		LOG.info("Shutting down Scheduler. Will wait {} {} for running tasks to complete.", SHUTDOWN_WAIT, SHUTDOWN_WAIT_TIMEUNIT);
		MoreExecutors.shutdownAndAwaitTermination(dueExecutor, 1, TimeUnit.MINUTES);
		MoreExecutors.shutdownAndAwaitTermination(detectDeadExecutor, 1, TimeUnit.MINUTES);

		final boolean result = MoreExecutors.shutdownAndAwaitTermination(executorService, SHUTDOWN_WAIT, SHUTDOWN_WAIT_TIMEUNIT);

		if (result) {
			LOG.info("Scheduler stopped.");
		} else {
			LOG.warn("Scheduler stopped, but some tasks did not complete.");
		}
	}

	public void addExecution(LocalDateTime exeecutionTime, TaskInstance taskInstance) {
		taskRepository.createIfNotExists(new Execution(exeecutionTime, taskInstance));
	}

	void executeDue() {
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
		final LocalDateTime oldAgeLimit = now.minus(heartbeatInterval.multipliedBy(4).toMillis(), ChronoUnit.MILLIS);
		List<Execution> oldExecutions = taskRepository.getOldExecutions(oldAgeLimit);

		if (!oldExecutions.isEmpty()) {
			oldExecutions.stream().forEach(execution -> {
				LOG.info("Found dead execution. Delegating handling to task. Execution: " + execution);
				execution.taskInstance.getTask().handleDeadExecution(execution, new ExecutionOperations(execution));
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
			taskRepository.updateHeartbeat(execution, now);
		});
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
				try {
					LOG.debug("Executing " + execution);
					task.execute(execution.taskInstance);
					LOG.debug("Execution done");
					task.complete(new ExecutionComplete(execution, clock.now(), ExecutionComplete.Result.OK), new ExecutionOperations(execution));

				} catch (RuntimeException unhandledException) {
					LOG.warn("Unhandled exception during execution. Treating as failure.", unhandledException);
					task.complete(new ExecutionComplete(execution, clock.now(), ExecutionComplete.Result.FAILED), new ExecutionOperations(execution));

				} catch (Throwable unhandledError) {
					LOG.error("Error during execution. Treating as failure.", unhandledError);
					task.complete(new ExecutionComplete(execution, clock.now(), ExecutionComplete.Result.FAILED), new ExecutionOperations(execution));

				}

			} catch (Throwable throwable) {
				statsRegistry.registerUnexpectedError();
				LOG.error("Failed while completing execution " + execution + ". Execution will likely remain scheduled and locked/picked. " +
						"If the task is resumable, the lock will be released after duration " + OLD_AGE + " and execution run again.", throwable);
			}
		}
	}

	public static Builder create(DataSource dataSource, SchedulerName schedulerName) {
		return new Builder(dataSource, schedulerName);
	}

	public static class Builder {

		private final DataSource dataSource;
		private final SchedulerName schedulerName;
		private int executorThreads = 10;
		private final List<Task> knownTasks = new ArrayList<>();
		private Waiter waiter = new Waiter(1000);
		private StatsRegistry statsRegistry = StatsRegistry.NOOP;
		private Duration heartbeatInterval = Duration.ofMinutes(5);

		public Builder(DataSource dataSource, SchedulerName schedulerName) {
			this.dataSource = dataSource;
			this.schedulerName = schedulerName;
		}

		public Builder addTask(Task task) {
			knownTasks.add(task);
			return this;
		}

		public Builder pollingInterval(long time, TimeUnit timeUnit) {
			waiter = new Waiter(timeUnit.toMillis(time));
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


	public class ExecutionOperations {

		private final Execution execution;

		public ExecutionOperations(Execution execution) {
			this.execution = execution;
		}

		public void stop() {
			taskRepository.remove(execution);
		}

		public void reschedule(LocalDateTime nextExecutionTime) {
			taskRepository.reschedule(execution, nextExecutionTime);
		}

	}

	public static class Waiter {
		private final long millis;

		public Waiter(long millis) {
			this.millis = millis;
		}

		public void doWait() {
			try {
				if (millis > 0) {
					Thread.sleep(millis);
				}
			} catch (InterruptedException e) {
				LOG.info("Interrupted. Assuming shutdown and continuing.");
			}
		}
	}

	public static class NoAvailableExecutors extends RuntimeException {
	}
}
