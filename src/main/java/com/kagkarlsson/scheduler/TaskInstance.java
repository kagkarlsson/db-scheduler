package com.kagkarlsson.scheduler;

import java.util.Objects;

public final class TaskInstance {

	private final Task task;
	private final String id;

	public TaskInstance(Task task, String id) {
		this.task = task;
		this.id = id;
	}

	public String getTaskId() {
		return task.getName() + "_" + id;
	}

	public Task getTask() {
		return task;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TaskInstance that = (TaskInstance) o;
		return Objects.equals(task.getName(), that.task.getName()) &&
				Objects.equals(id, that.id);
	}

	@Override
	public int hashCode() {
		return Objects.hash(task.getName(), id);
	}

	@Override
	public String toString() {
		return "TaskInstance{" +
				"task=" + task +
				", id='" + id + '\'' +
				'}';
	}
}
