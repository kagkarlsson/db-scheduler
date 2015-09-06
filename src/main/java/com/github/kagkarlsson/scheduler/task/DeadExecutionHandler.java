package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.Execution;
import com.github.kagkarlsson.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public interface DeadExecutionHandler {
	void deadExecution(Execution execution, Scheduler.ExecutionOperations executionOperations);

	class RescheduleDeadExecution implements DeadExecutionHandler {
		private static final Logger LOG = LoggerFactory.getLogger(RescheduleDeadExecution.class);

		@Override
		public void deadExecution(Execution execution, Scheduler.ExecutionOperations executionOperations) {
			final LocalDateTime now = LocalDateTime.now();
			LOG.warn("Rescheduling dead execution: " + execution + " to " + now);
			executionOperations.reschedule(now);
		}
	}

	class CancelDeadExecution implements DeadExecutionHandler {
		private static final Logger LOG = LoggerFactory.getLogger(RescheduleDeadExecution.class);

		@Override
		public void deadExecution(Execution execution, Scheduler.ExecutionOperations executionOperations) {
			LOG.error("Cancelling dead execution: " + execution);
			executionOperations.stop();
		}
	}
}
