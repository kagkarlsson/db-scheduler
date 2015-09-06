package com.github.kagkarlsson.scheduler.task;

public class OneTimeTask extends Task {

	public OneTimeTask(String name, ExecutionHandler handler) {
		super(name, handler, new CompletionHandler.OnCompleteRemove(), new DeadExecutionHandler.RescheduleDeadExecution());
	}

}
