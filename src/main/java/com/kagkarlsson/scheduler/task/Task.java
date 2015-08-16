package com.kagkarlsson.scheduler.task;

import com.kagkarlsson.scheduler.Scheduler;

public abstract class Task {
	protected final String name;
	private final ExecutionHandler executionHandler;
	private final CompletionHandler completionHandler;

	public Task(String name, ExecutionHandler executionHandler, CompletionHandler completionHandler) {
		this.name = name;
		this.executionHandler = executionHandler;
		this.completionHandler = completionHandler;
	}

	public String getName() {
		return name;
	}

	public void complete(ExecutionComplete executionComplete, Scheduler.ExecutionFinishedOperations executionFinishedOperations) {
		completionHandler.complete(executionComplete, executionFinishedOperations);
	}

	public TaskInstance instance(String id) {
		return new TaskInstance(this, id);
	}

	public void execute(TaskInstance taskInstance) {
		executionHandler.execute(taskInstance);
	}

	@Override
	public String toString() {
		return "Task " +
				"task=" + getName();
	}

}

