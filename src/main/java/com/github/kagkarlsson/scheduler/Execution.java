package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.TaskInstance;

import java.time.LocalDateTime;
import java.util.Objects;

public final class Execution {
	public final TaskInstance taskInstance;
	public final LocalDateTime executionTime;
	public boolean picked;

	public Execution(LocalDateTime executionTime, TaskInstance taskInstance) {
		this.executionTime = executionTime;
		this.taskInstance = taskInstance;
		picked = false;
	}

	public void setPicked() {
		this.picked = true;
	}

	public LocalDateTime getExecutionTime() {
		return executionTime;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Execution execution = (Execution) o;
		return Objects.equals(executionTime, execution.executionTime) &&
				Objects.equals(taskInstance, execution.taskInstance);
	}

	@Override
	public int hashCode() {
		return Objects.hash(executionTime, taskInstance);
	}


	@Override
	public String toString() {
		return "Execution: " +
				"task=" + taskInstance.getTask().getName() +
				", id=" + taskInstance.getId() +
				", executionTime=" + executionTime;
	}
}
