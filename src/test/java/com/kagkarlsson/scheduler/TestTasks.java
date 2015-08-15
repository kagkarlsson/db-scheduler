package com.kagkarlsson.scheduler;

import com.kagkarlsson.scheduler.task.TaskInstance;

import java.util.function.Consumer;

public class TestTasks {

	public static final Consumer<TaskInstance> DO_NOTHING = (taskInstance -> {});

	public static class CountingHandler implements Consumer<TaskInstance> {
		public int timesExecuted = 0;

		@Override
		public void accept(TaskInstance taskInstance) {
			this.timesExecuted++;
		}
	}

}
