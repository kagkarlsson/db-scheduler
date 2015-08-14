package com.kagkarlsson.scheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kagkarlsson.scheduler.executors.CapacityLimitedExecutorService;
import com.kagkarlsson.scheduler.executors.LimitedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.StringWriter;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Scheduler {

	private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	public static final int SHUTDOWN_WAIT = 30;
	public static final TimeUnit SHUTDOWN_WAIT_TIMEUNIT = TimeUnit.MINUTES;
	private static final Duration OLD_AGE = Duration.ofHours(6);
	private final Clock clock;
	private final TaskRepository taskRepository;
	private final CapacityLimitedExecutorService executorService;
	private final Name name;
	private final Waiter waiter;
	private final Waiter detectDeadWaiter;
	private final Consumer<String> warnLogger;
	private final ExecutorService dueExecutor;
	private final ExecutorService detectDeadExecutor;
	private boolean shutdownRequested = false;

	Scheduler(Clock clock, TaskRepository taskRepository, CapacityLimitedExecutorService executorService, Name name,
			  Waiter waiter, Waiter detectDeadWaiter, Consumer<String> warnLogger) {
		this.clock = clock;
		this.taskRepository = taskRepository;
		this.executorService = executorService;
		this.name = name;
		this.waiter = waiter;
		this.detectDeadWaiter = detectDeadWaiter;
		this.warnLogger = warnLogger;
		this.dueExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(name.getName() + "-execute-due").build());
		this.detectDeadExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(name.getName() + "-detect-dead").build());
	}

	public void start() {
		LOG.info("Starting scheduler");

		dueExecutor.submit(() -> {
			while (!shutdownRequested) {
				try {
					if (executorService.hasFreeExecutor()) {
						executeDue();
					}

					waiter.doWait();

				} catch (Exception e) {
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
					LOG.error("Unhandled exception", e);
				}
			}
		});
	}

	public void stop() {
		shutdownRequested = true;
		try {
			LOG.info("Shutting down Scheduler. Will wait {} {} for running tasks to complete.", SHUTDOWN_WAIT, SHUTDOWN_WAIT_TIMEUNIT);
			dueExecutor.shutdownNow();
			detectDeadExecutor.shutdownNow();
			executorService.shutdown();

			detectDeadExecutor.awaitTermination(5, TimeUnit.SECONDS);
			dueExecutor.awaitTermination(SHUTDOWN_WAIT, SHUTDOWN_WAIT_TIMEUNIT);
			boolean result = executorService.awaitTermination(SHUTDOWN_WAIT, SHUTDOWN_WAIT_TIMEUNIT);
			if (result) {
				LOG.info("Done waiting for executing tasks.");
			} else {
				LOG.warn("Some tasks did not complete. Forcing shutdown.");
				executorService.shutdownNow();
				executorService.awaitTermination(10, TimeUnit.SECONDS);
			}

		} catch (InterruptedException e) {
			LOG.warn("Interrupted while waiting for running tasks to complete.", e);
		}
	}

	public void schedule(LocalDateTime exeecutionTime, TaskInstance taskInstance) {
		taskRepository.createIfNotExists(new Execution(exeecutionTime, taskInstance));
	}

	void executeDue() {
		LocalDateTime now = clock.now();
		List<Execution> dueExecutions = taskRepository.getDue(now);

		int count = 0;
		LOG.info("Found {} taskinstances due for execution", dueExecutions.size());
		for (Execution e : dueExecutions) {
			if (!executorService.hasFreeExecutor()) {
				LOG.debug("No free executors. Will not run remaining {} due executions.", dueExecutions.size() - count);
				return;
			}

			count++;
			if (taskRepository.pick(e)) {
				executorService.submit(() -> {
					try {
						Run.logExceptions(LOG, () -> {
							LOG.debug("Executing " + e);
							e.taskInstance.getTask().execute(e.taskInstance);
						}).run();
					} finally {
						Run.logExceptions(LOG,
								() -> {
									LOG.trace("Completing " + e);
									e.taskInstance.getTask().complete(new Task.ExecutionResult(e, clock.now()), new ExecutionFinishedOperations(e));
								}).run();
					}
				});
			}
		}
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
					.map(execution -> "Execution: " + execution.taskInstance.getTaskAndInstance() + " was due " + execution.exeecutionTime + "\n")
					.forEach(warning::append);
			warnLogger.accept(warning.toString());
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
		private Waiter detectDeadWaiter = new Waiter(TimeUnit.HOURS.toMillis(1));;
		private Consumer<String> warnLogger = WARN_LOGGER;

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

		Scheduler build() {
			final TaskResolver taskResolver = new TaskResolver(knownTasks, TaskResolver.OnCannotResolve.WARN_ON_UNRESOLVED);
			final JdbcTaskRepository taskRepository = new JdbcTaskRepository(dataSource, taskResolver);
			final FixedName name = new FixedName(this.name);

			final LimitedThreadPool executorService = new LimitedThreadPool(executorThreads, new ThreadFactoryBuilder().setNameFormat(name.getName() + "-%d").build());
			return new Scheduler(new SystemClock(), taskRepository, executorService, name, waiter, detectDeadWaiter, warnLogger);
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
