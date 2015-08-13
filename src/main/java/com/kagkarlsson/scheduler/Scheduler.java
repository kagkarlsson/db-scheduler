package com.kagkarlsson.scheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kagkarlsson.scheduler.executors.CapacityLimitedExecutorService;
import com.kagkarlsson.scheduler.executors.LimitedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Scheduler {

	private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	public static final int SHUTDOWN_WAIT = 30;
	public static final TimeUnit SHUTDOWN_WAIT_TIMEUNIT = TimeUnit.MINUTES;
	private final Clock clock;
	private final TaskRepository taskRepository;
	private final CapacityLimitedExecutorService executorService;
	private final Name name;
	private Map<String, Task> knownTasks = new HashMap<>();
	private boolean shutdownRequested = false;

	public Scheduler(Clock clock, TaskRepository taskRepository, int executorThreads, Name name) {
		this(clock, taskRepository, new LimitedThreadPool(executorThreads, new ThreadFactoryBuilder().setNameFormat(name.getName() + "-%d").build()), name);
	}

	Scheduler(Clock clock, TaskRepository taskRepository, CapacityLimitedExecutorService executorService, Name name) {
		this.clock = clock;
		this.taskRepository = taskRepository;
		this.executorService = executorService;
		this.name = name;
	}

	public void start() {
		LOG.info("Starting scheduler");

		new Thread(name.getName() + "-main-thread-") {
			@Override
			public void run() {
				while(!shutdownRequested) {
					try {
						if (executorService.hasFreeExecutor()) {
							executeDue();
						}

						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							LOG.info("Interrupted. Assuming shutdown and continuing.");
						}

					} catch (Exception e) {
						LOG.error("Unhandled exception", e);
					}
				}
			}
		}.start();
	}

	public void stop() {
		shutdownRequested = true;
		try {
			LOG.info("Shutting down Scheduler. Will wait {} {} for running tasks to complete.", SHUTDOWN_WAIT, SHUTDOWN_WAIT_TIMEUNIT);
			executorService.shutdown();
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

	public void addTask(Task task) {
		knownTasks.put(task.getName(), task);
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

			// -> attempt lock here
			//    if lock
			//      execute
			//      release lock

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
									e.taskInstance.getTask().complete(e, clock.now(), new TaskInstanceOperations(e));
								}).run();
					}
				});
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

		public Builder(DataSource dataSource) {
			this.dataSource = dataSource;
		}

		public Builder addHandler(Task task) {
			knownTasks.add(task);
			return this;
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		Scheduler build() {
			final TaskResolver taskResolver = new TaskResolver(knownTasks, TaskResolver.OnCannotResolve.WARN_ON_UNRESOLVED);
			final JdbcTaskRepository taskRepository = new JdbcTaskRepository(dataSource, taskResolver);

			return new Scheduler(new SystemClock(), taskRepository, executorThreads, new FixedName(name));
		}
	}


	public class TaskInstanceOperations {

		private final Execution execution;

		public TaskInstanceOperations(Execution execution) {
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

}
