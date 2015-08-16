package com.kagkarlsson.scheduler;

import com.kagkarlsson.scheduler.task.ExecutionHandler;
import com.kagkarlsson.scheduler.task.TaskInstance;

import java.util.function.Consumer;

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
