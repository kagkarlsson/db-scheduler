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

import com.github.kagkarlsson.scheduler.Execution;
import com.github.kagkarlsson.scheduler.Scheduler;

public abstract class Task {
	protected final String name;
	private final ExecutionHandler executionHandler;
	private final CompletionHandler completionHandler;
	private final DeadExecutionHandler deadExecutionHandler;

	public Task(String name, ExecutionHandler executionHandler, CompletionHandler completionHandler, DeadExecutionHandler deadExecutionHandler) {
		this.name = name;
		this.executionHandler = executionHandler;
		this.completionHandler = completionHandler;
		this.deadExecutionHandler = deadExecutionHandler;
	}

	public String getName() {
		return name;
	}

	public TaskInstance instance(String id) {
		return new TaskInstance(this, id);
	}

	public void execute(TaskInstance taskInstance) {
		executionHandler.execute(taskInstance);
	}

	public void complete(ExecutionComplete executionComplete, Scheduler.ExecutionOperations executionOperations) {
		completionHandler.complete(executionComplete, executionOperations);
	}

	public void handleDeadExecution(Execution execution, Scheduler.ExecutionOperations executionOperations) {
		deadExecutionHandler.deadExecution(execution, executionOperations);
	}

	@Override
	public String toString() {
		return "Task " +
				"task=" + getName();
	}
}

