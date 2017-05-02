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

import com.github.kagkarlsson.scheduler.SchedulerState;

public class ExecutionContext {

	private SchedulerState schedulerState;
	private final Execution execution;

	public ExecutionContext(SchedulerState schedulerState, Execution execution) {
		this.schedulerState = schedulerState;
		this.execution = execution;
	}

	public SchedulerState getSchedulerState() {
		return schedulerState;
	}

	public Execution getExecution() {
		return execution;
	}
}
