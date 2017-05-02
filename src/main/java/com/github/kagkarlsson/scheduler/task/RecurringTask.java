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

import static com.github.kagkarlsson.scheduler.task.Task.Serializer.NO_SERIALIZER;

public abstract class RecurringTask extends Task<Void> implements OnStartup {

	public static final String INSTANCE = "recurring";

	public RecurringTask(String name, Schedule schedule) {
		super(name, new OnCompleteReschedule(schedule), new RescheduleDeadExecution(), NO_SERIALIZER);
	}

	@Override
	public void onStartup(Scheduler scheduler) {
		scheduler.schedule(Instant.now(), this.instance(INSTANCE));
	}

}
