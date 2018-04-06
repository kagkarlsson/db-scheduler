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

import java.time.Instant;
import java.util.Objects;

// TODO: possibly make Execution generic
@SuppressWarnings("rawtypes")
public final class Execution {
	public final TaskInstance taskInstance;
	public final Instant executionTime;
	public boolean picked;
	public String pickedBy;
	public Instant lastHeartbeat;
	public long version;
	public Instant lastFailure;
	public Instant lastSuccess;

	public Execution(Instant executionTime, TaskInstance taskInstance) {
		this(executionTime, taskInstance, false, null, null, null, null, 1L);
	}

	public Execution(Instant executionTime, TaskInstance taskInstance, boolean picked, String pickedBy,
					 Instant lastSuccess, Instant lastFailure, Instant lastHeartbeat, long version) {
		this.executionTime = executionTime;
		this.taskInstance = taskInstance;
		this.picked = picked;
		this.pickedBy = pickedBy;
		this.lastFailure = lastFailure;
		this.lastSuccess = lastSuccess;
		this.lastHeartbeat = lastHeartbeat;
		this.version = version;
	}

	public void setPicked(String pickedBy, Instant timePicked) {
		this.pickedBy = pickedBy;
		this.picked = true;
		this.lastHeartbeat = timePicked;
	}

	public Instant getExecutionTime() {
		return executionTime;
	}

	public boolean isPicked() {
		return picked;
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
				"task=" + taskInstance.getTaskName() +
				", id=" + taskInstance.getId() +
				", executionTime=" + executionTime +
				", picked=" + picked +
				", pickedBy=" + pickedBy +
				", lastHeartbeat=" + lastHeartbeat +
				", version=" + version;
	}
}
