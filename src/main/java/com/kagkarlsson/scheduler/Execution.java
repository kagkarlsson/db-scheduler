package com.kagkarlsson.scheduler;

import java.time.LocalDateTime;
import java.util.Objects;

public final class Execution {
	public final TaskInstance taskInstance;
	public final LocalDateTime exeecutionTime;
	public boolean picked;

	public Execution(LocalDateTime exeecutionTime, TaskInstance taskInstance) {
		this.exeecutionTime = exeecutionTime;
		this.taskInstance = taskInstance;
		picked = false;
	}

	public void setPicked() {
		this.picked = true;
	}

	public LocalDateTime getExeecutionTime() {
		return exeecutionTime;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Execution execution = (Execution) o;
		return Objects.equals(exeecutionTime, execution.exeecutionTime) &&
				Objects.equals(taskInstance, execution.taskInstance);
	}

	@Override
	public int hashCode() {
		return Objects.hash(exeecutionTime, taskInstance);
	}

	@Override
	public String toString() {
		return "Execution{" +
				"taskInstance=" + taskInstance +
				", exeecutionTime=" + exeecutionTime +
				", picked=" + picked +
				'}';
	}
}
