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

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.CompletionHandler.OnCompleteReschedule;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler.RescheduleDeadExecution;

import java.time.Instant;

public abstract class RecurringTask<T> extends Task<T> implements OnStartup {

	public static final String INSTANCE = "recurring";
	private final OnCompleteReschedule<T> onComplete;
	private final T initialData;

	public RecurringTask(String name, Schedule schedule, Class<T> dataClass, T initialData) {
		super(name, dataClass, new FailureHandler.OnFailureReschedule<>(schedule), new RescheduleDeadExecution<>());
		onComplete = new OnCompleteReschedule<>(schedule);
		this.initialData = initialData;
	}

	@Override
	public void onStartup(Scheduler scheduler) {
		scheduler.schedule(this.instance(INSTANCE, initialData), Instant.now());
	}

	@Override
	public CompletionHandler<T> execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
		executeRecurringly(taskInstance, executionContext);
		return onComplete;
	}

	public abstract void executeRecurringly(TaskInstance<T> taskInstance, ExecutionContext executionContext);
}
