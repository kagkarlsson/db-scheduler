package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.Execution;
import com.github.kagkarlsson.scheduler.Scheduler;

public abstract class Task {
	protected final String name;
	private final ExecutionHandler executionHandler;
	private final CompletionHandler completionHandler;
	private final DeadExecutionHandler deadExecutionHandler;

	public Task(String name, ExecutionHandler executionHandler, CompletionHandler completionHandler, DeadExecutionHandler deadExecutionHandler) {
		this.name = name;
		this.executionHandler = executionHandler;
		this.completionHandler = completionHandler;
		this.deadExecutionHandler = deadExecutionHandler;
	}

	public String getName() {
		return name;
	}

	public TaskInstance instance(String id) {
		return new TaskInstance(this, id);
	}

	public void execute(TaskInstance taskInstance) {
		executionHandler.execute(taskInstance);
	}

	public void complete(ExecutionComplete executionComplete, Scheduler.ExecutionOperations executionOperations) {
		completionHandler.complete(executionComplete, executionOperations);
	}

	public void handleDeadExecution(Execution execution, Scheduler.ExecutionOperations executionOperations) {
		deadExecutionHandler.deadExecution(execution, executionOperations);
	}

	@Override
	public String toString() {
		return "Task " +
				"task=" + getName();
	}
}

