package com.kagkarlsson.scheduler;

import com.kagkarlsson.scheduler.executors.CapacityLimitedExecutorService;
import com.kagkarlsson.scheduler.executors.LimitedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
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
	private Map<String, Task> knownTasks = new HashMap<>();
	private boolean shutdownRequested = false;

	public Scheduler(Clock clock, TaskRepository taskRepository, int executorThreads) {
		this(clock, taskRepository, new LimitedThreadPool(executorThreads));
	}

	Scheduler(Clock clock, TaskRepository taskRepository, CapacityLimitedExecutorService executorService) {
		this.clock = clock;
		this.taskRepository = taskRepository;
		this.executorService = executorService;
	}

	public void start() {
		LOG.info("Starting scheduler");
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
						Run.logExceptions(LOG, () -> e.taskInstance.getTask().execute(e.taskInstance)).run();
					} finally {
						Run.logExceptions(LOG,
								() -> e.taskInstance.getTask().complete(e, clock.now(), new TaskInstanceOperations(e))).run();
					}
				});
			}
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

}
