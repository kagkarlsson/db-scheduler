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

import java.time.Duration;

public abstract class OneTimeTask<T> extends Task<T> {

	private static final CompletionHandler ON_COMPLETE_REMOVE = new CompletionHandler.OnCompleteRemove();
	private static final FailureHandler.OnFailureRetryLater ON_FAILURE_RETRY = new FailureHandler.OnFailureRetryLater(Duration.ofMinutes(5));

	public OneTimeTask(String name) {
		super(name, ON_FAILURE_RETRY, new DeadExecutionHandler.RescheduleDeadExecution());
	}

	public OneTimeTask(String name, Serializer<T> serializer) {
		super(name, ON_FAILURE_RETRY, new DeadExecutionHandler.RescheduleDeadExecution(), serializer);
	}

	@Override
	public CompletionHandler execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
		executeOnce(taskInstance, executionContext);
		return ON_COMPLETE_REMOVE;
	}

	public abstract void executeOnce(TaskInstance<T> taskInstance, ExecutionContext executionContext);

}
