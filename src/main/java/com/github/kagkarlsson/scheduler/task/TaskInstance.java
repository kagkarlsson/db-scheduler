/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task;

import java.util.Objects;

public final class TaskInstance {

	private final Task task;
	private final String id;

	public TaskInstance(Task task, String id) {
		this.task = task;
		this.id = id;
	}

	public String getTaskAndInstance() {
		return task.getName() + "_" + id;
	}

	public Task getTask() {
		return task;
	}

	public String getTaskName() {
		return task.getName();
	}

	public String getId() {
		return id;
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
		return "TaskInstance: " +
				"task=" + task.getName() +
				", id=" + id;
	}
}
