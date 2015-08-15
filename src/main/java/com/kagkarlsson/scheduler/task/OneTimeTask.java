package com.kagkarlsson.scheduler.task;

import com.kagkarlsson.scheduler.Scheduler;

import java.util.function.Consumer;

public class OneTimeTask extends Task {
	private final Consumer<TaskInstance> handler;

	public OneTimeTask(String name, Consumer<TaskInstance> handler) {
		super(name);
		this.handler = handler;
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
	public void complete(ExecutionResult executionResult, Scheduler.ExecutionFinishedOperations executionFinishedOperations) {
		executionFinishedOperations.stop();
	}

}
