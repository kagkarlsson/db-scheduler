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
package com.github.kagkarlsson.scheduler.task.helper;

import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;

import java.time.Duration;

@Deprecated
public class ComposableTask {

	public static RecurringTask<Void> recurringTask(String name, Schedule schedule, VoidExecutionHandler<Void> executionHandler) {
		return new RecurringTask<Void>(name, schedule, Void.class) {
			@Override
			public void executeRecurringly(TaskInstance<Void> taskInstance, ExecutionContext executionContext) {
				executionHandler.execute(taskInstance, executionContext);
			}
		};
	}

	public static <T> OneTimeTask<T> onetimeTask(String name, Class<T> dataClass, VoidExecutionHandler<T> executionHandler) {
		return new OneTimeTask<T>(name, dataClass) {
			@Override
			public void executeOnce(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
				executionHandler.execute(taskInstance, executionContext);
			}
		};
	}

	public static <T> Task<T> customTask(String name, Class<T> dataClass, CompletionHandler<T> completionHandler, VoidExecutionHandler<T> executionHandler) {
		return new Task<T>(name, dataClass, new FailureHandler.OnFailureRetryLater<>(Duration.ofMinutes(5)), new DeadExecutionHandler.ReviveDeadExecution<>()) {
			@Override
			public CompletionHandler<T> execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
				executionHandler.execute(taskInstance, executionContext);
				return completionHandler;
			}
		};
	}

	public static <T> Task<T> customTask(String name, Class<T> dataClass, CompletionHandler<T> completionHandler, FailureHandler<T> failureHandler, VoidExecutionHandler<T> executionHandler) {
		return new Task<T>(name, dataClass, failureHandler, new DeadExecutionHandler.ReviveDeadExecution<>()) {
			@Override
			public CompletionHandler<T> execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
				executionHandler.execute(taskInstance, executionContext);
				return completionHandler;
			}
		};
	}

}
