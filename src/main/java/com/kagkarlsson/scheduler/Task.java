package com.kagkarlsson.scheduler;

import java.time.LocalDateTime;

public abstract class Task {

	protected final String name;

	public Task(String name) {
		this.name = name;
	}

	public abstract String getName();
	public abstract TaskInstance instance(String id);
	public abstract void execute(TaskInstance taskInstance);
	public abstract void complete(Execution taskInstance, LocalDateTime timeDone, Scheduler.TaskInstanceOperations taskInstanceOperations);

	@Override
	public String toString() {
		return "Task{" +
				"task=" + getName() +
				'}';
	}
}

