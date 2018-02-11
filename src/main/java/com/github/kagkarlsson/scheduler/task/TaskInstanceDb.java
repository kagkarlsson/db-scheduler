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

public final class TaskInstanceDb implements TaskInstanceId {

	private final String taskName;
	private final String id;
	private final byte[] data;

	public TaskInstanceDb(String taskName, String id) {
		this(taskName, id, null);
	}

	public TaskInstanceDb(String taskName, String id, byte[] data) {
		this.taskName = taskName;
		this.id = id;
		this.data = data;
	}

	public String getTaskAndInstance() {
		return taskName + "_" + id;
	}

	public String getTaskName() {
		return taskName;
	}

	@Override
	public String getId() {
		return id;
	}

	public byte[] getData() {
		return data;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((taskName == null) ? 0 : taskName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TaskInstanceDb other = (TaskInstanceDb) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (taskName == null) {
			if (other.taskName != null)
				return false;
		} else if (!taskName.equals(other.taskName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TaskInstance: " + "task=" + taskName + ", id=" + id;
	}

}
