package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.Execution;

import java.time.LocalDateTime;

public class ExecutionComplete {
	private Execution execution;
	private LocalDateTime timeDone;
	private final Result result;

	public ExecutionComplete(Execution execution, LocalDateTime timeDone, Result result) {
		this.execution = execution;
		this.timeDone = timeDone;
		this.result = result;
	}

	public Execution getExecution() {
		return execution;
	}

	public LocalDateTime getTimeDone() {
		return timeDone;
	}

	public Result getResult() {
		return result;
	}

	public enum Result {
		OK,
		FAILED;
	}
}
