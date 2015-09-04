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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class Scheduler {

	private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	public static final int SHUTDOWN_WAIT = 30;
	public static final TimeUnit SHUTDOWN_WAIT_TIMEUNIT = TimeUnit.MINUTES;
	private static final Duration OLD_AGE = Duration.ofHours(6);
	private final Clock clock;
	private final TaskRepository taskRepository;
	private final int maxThreads;
	private final ExecutorService executorService;
	private final Name name;
	private final Waiter waiter;
	private final Waiter detectDeadWaiter;
	private final Consumer<String> warnLogger;
	private final StatsRegistry statsRegistry;
	private final ExecutorService dueExecutor;
	private final ExecutorService detectDeadExecutor;
	final Semaphore executorsSemaphore;

	private boolean shutdownRequested = false;

	Scheduler(Clock clock, TaskRepository taskRepository, int maxThreads, Name name,
			  Waiter waiter, Waiter detectDeadWaiter, Consumer<String> warnLogger, StatsRegistry statsRegistry) {
		this(clock, taskRepository, maxThreads, defaultExecutorService(maxThreads, name), name, waiter, detectDeadWaiter, warnLogger, statsRegistry);
	}

	private static ExecutorService defaultExecutorService(int maxThreads, Name name) {
		return Executors.newFixedThreadPool(maxThreads, new ThreadFactoryBuilder().setNameFormat(name.getName() + "-%d").build());
	}

	Scheduler(Clock clock, TaskRepository taskRepository, int maxThreads, ExecutorService executorService, Name name,
			  Waiter waiter, Waiter detectDeadWaiter, Consumer<String> warnLogger, StatsRegistry statsRegistry) {
		this.clock = clock;
		this.taskRepository = taskRepository;
		this.maxThreads = maxThreads;
		this.executorService = executorService;
		this.name = name;
		this.waiter = waiter;
		this.detectDeadWaiter = detectDeadWaiter;
		this.warnLogger = warnLogger;
		this.statsRegistry = statsRegistry;
		this.dueExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(name.getName() + "-execute-due").build());
		this.detectDeadExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(name.getName() + "-detect-dead").build());
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

				} catch (Exception e) {
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

				} catch (Exception e) {
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
			LOG.info("Done waiting for executing tasks.");
		} else {
			LOG.warn("Some tasks did not complete.");
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

			if (aquireExecutorAndPickExecution(e)) {
				count++;
				CompletableFuture
						.runAsync(new ExecuteTask(e), executorService)
						.thenRun(() -> releaseExecutor());
			} else {
				LOG.debug("No available executors. Skipping {} executions", dueExecutions.size() - count);
				return;
			}

		}
	}

	private boolean aquireExecutorAndPickExecution(Execution execution) {
		if (executorsSemaphore.tryAcquire()) {
			try {
				boolean successfulPick = taskRepository.pick(execution);

				if (!successfulPick) {
					executorsSemaphore.release();
				}
				return successfulPick;

			} catch (Throwable t) {
				executorsSemaphore.release();
				throw t;
			}
		}

		return false;
	}

	private void releaseExecutor() {
		executorsSemaphore.release();
	}

	void detectDeadExecutions() {
		LOG.info("Checking for dead executions.");
		LocalDateTime now = clock.now();
		List<Execution> oldExecutions = taskRepository.getOldExecutions(now.minus(OLD_AGE));

		if (!oldExecutions.isEmpty()) {
			StringBuilder warning = new StringBuilder();
			warning.append("There are " + oldExecutions.size() + " old executions. They are either long running executions, " +
					"or they have died and are non-resumable. Old executions:\n");
			oldExecutions.stream()
					.map(execution -> "Execution: " + execution.taskInstance.getTaskAndInstance() + " was due " + execution.executionTime + "\n")
					.forEach(warning::append);
			warnLogger.accept(warning.toString());
		}
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
					task.complete(new ExecutionComplete(execution, clock.now(), ExecutionComplete.Result.OK), new ExecutionFinishedOperations(execution));

				} catch (RuntimeException unhandledException) {
					LOG.warn("Unhandled exception during execution. Treating as failure.", unhandledException);
					task.complete(new ExecutionComplete(execution, clock.now(), ExecutionComplete.Result.FAILED), new ExecutionFinishedOperations(execution));

				} catch (Throwable unhandledError) {
					LOG.error("Error during execution. Treating as failure.", unhandledError);
					task.complete(new ExecutionComplete(execution, clock.now(), ExecutionComplete.Result.FAILED), new ExecutionFinishedOperations(execution));

				}

			} catch (Throwable throwable) {
				statsRegistry.registerUnexpectedError();
				LOG.error("Failed while completing execution " + execution + ". Execution will likely remain scheduled and locked/picked. " +
						"If the task is resumable, the lock will be released after duration " + OLD_AGE + " and execution run again.", throwable);
			}
		}
	}

	public static Builder create(DataSource dataSource) {
		return new Builder(dataSource);
	}

	public static class Builder {

		private final DataSource dataSource;
		private int executorThreads = 10;
		private List<Task> knownTasks = new ArrayList<>();
		private String name;
		private Waiter waiter = new Waiter(1000);
		private Waiter detectDeadWaiter = new Waiter(TimeUnit.HOURS.toMillis(1));
		;
		private Consumer<String> warnLogger = WARN_LOGGER;
		private StatsRegistry statsRegistry = StatsRegistry.NOOP;

		public Builder(DataSource dataSource) {
			this.dataSource = dataSource;
		}

		public Builder addTask(Task task) {
			knownTasks.add(task);
			return this;
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder pollingInterval(long time, TimeUnit timeUnit) {
			waiter = new Waiter(timeUnit.toMillis(time));
			return this;
		}

		public Builder detectDeadInterval(long time, TimeUnit timeUnit) {
			detectDeadWaiter = new Waiter(timeUnit.toMillis(time));
			return this;
		}

		public Builder statsRegistry(StatsRegistry statsRegistry) {
			this.statsRegistry = statsRegistry;
			return this;
		}

		Scheduler build() {
			final TaskResolver taskResolver = new TaskResolver(knownTasks, TaskResolver.OnCannotResolve.WARN_ON_UNRESOLVED);
			final JdbcTaskRepository taskRepository = new JdbcTaskRepository(dataSource, taskResolver);
			final FixedName name = new FixedName(this.name);

			return new Scheduler(new SystemClock(), taskRepository, executorThreads, name, waiter, detectDeadWaiter, warnLogger, statsRegistry);
		}
	}


	public class ExecutionFinishedOperations {

		private final Execution execution;

		public ExecutionFinishedOperations(Execution execution) {
			this.execution = execution;
		}

		public void stop() {
			taskRepository.remove(execution);
		}

		public void reschedule(LocalDateTime nextExecutionTime) {
			taskRepository.reschedule(execution, nextExecutionTime);
		}

	}

	public interface Name {
		String getName();
	}

	public static class FixedName implements Name {
		private final String name;

		public FixedName(String name) {
			this.name = name;
		}

		@Override
		public String getName() {
			return name;
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

	public static Consumer<String> WARN_LOGGER = (String message) -> LOG.warn(message);

}
