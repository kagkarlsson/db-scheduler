package com.kagkarlsson.scheduler.task;

import com.kagkarlsson.scheduler.Execution;
import com.kagkarlsson.scheduler.Scheduler;

import java.time.LocalDateTime;

public abstract class Task {

	protected final String name;

	public Task(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public abstract TaskInstance instance(String id);
	public abstract void execute(TaskInstance taskInstance);

	public abstract void complete(ExecutionResult executionResult, Scheduler.ExecutionFinishedOperations executionFinishedOperations);

	@Override
	public String toString() {
		return "Task{" +
				"task=" + getName() +
				'}';
	}

	public static class ExecutionResult {
		private Execution execution;
		private LocalDateTime timeDone;

		public ExecutionResult(Execution execution, LocalDateTime timeDone) {
			this.execution = execution;
			this.timeDone = timeDone;
		}

		public Execution getExecution() {
			return execution;
		}

		public LocalDateTime getTimeDone() {
			return timeDone;
		}
	}
}

