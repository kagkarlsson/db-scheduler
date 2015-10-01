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

public class ComposableTask {

	public static RecurringTask recurringTask(String name, Schedule schedule, Runnable executionHandler) {
		return new RecurringTask(name, schedule) {
			@Override
			public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
				executionHandler.run();
			}
		};
	}

	public static OneTimeTask onetimeTask(String name, Runnable executionHandler) {
		return new OneTimeTask(name) {
			@Override
			public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
				executionHandler.run();
			}
		};
	}

	public static Task customTask(String name, CompletionHandler completionHandler, Runnable executionHandler) {
		return new Task(name, completionHandler, new DeadExecutionHandler.RescheduleDeadExecution()) {
			@Override
			public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
				executionHandler.run();
			}
		};
	}
}
