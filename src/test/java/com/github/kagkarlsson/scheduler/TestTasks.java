package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.ExecutionHandler;
import com.github.kagkarlsson.scheduler.task.TaskInstance;

public class TestTasks {

	public static final ExecutionHandler DO_NOTHING = (taskInstance -> {});

	public static class CountingHandler implements ExecutionHandler {
		public int timesExecuted = 0;

		@Override
		public void execute(TaskInstance taskInstance) {
			this.timesExecuted++;
		}
	}

}
