package com.kagkarlsson.scheduler.task;

public interface ExecutionHandler {
	void execute(TaskInstance taskInstance);
}
