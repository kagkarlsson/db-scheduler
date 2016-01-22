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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public interface DeadExecutionHandler {
	void deadExecution(Execution execution, ExecutionOperations executionOperations);

	class RescheduleDeadExecution implements DeadExecutionHandler {
		private static final Logger LOG = LoggerFactory.getLogger(RescheduleDeadExecution.class);

		@Override
		public void deadExecution(Execution execution, ExecutionOperations executionOperations) {
			final LocalDateTime now = LocalDateTime.now();
			LOG.warn("Rescheduling dead execution: " + execution + " to " + now);
			executionOperations.reschedule(new ExecutionComplete(execution, now, ExecutionComplete.Result.FAILED), now);
		}
	}

	class CancelDeadExecution implements DeadExecutionHandler {
		private static final Logger LOG = LoggerFactory.getLogger(RescheduleDeadExecution.class);

		@Override
		public void deadExecution(Execution execution, ExecutionOperations executionOperations) {
			LOG.error("Cancelling dead execution: " + execution);
			executionOperations.remove();
		}
	}
}
