package com.kagkarlsson.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.Consumer;

public class RecurringTask extends Task {

	private static final Logger LOG = LoggerFactory.getLogger(RecurringTask.class);
	private final Duration duration;
	private final Consumer<TaskInstance> handler;

	public RecurringTask(String name, Duration duration, Consumer<TaskInstance> handler) {
		super(name);
		this.duration = duration;
		this.handler = handler;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public TaskInstance instance(String id) {
		return new TaskInstance(this, id);
	}

	@Override
	public void execute(TaskInstance taskInstance) {
		handler.accept(taskInstance);
	}

	@Override
	public void complete(Execution taskInstance, LocalDateTime timeDone, Scheduler.TaskInstanceOperations taskInstanceOperations) {
		LocalDateTime nextExecution = timeDone.plus(duration);
		LOG.debug("Rescheduling task {} to {}", taskInstance.taskInstance, nextExecution);
		taskInstanceOperations.reschedule(nextExecution);
	}

}
