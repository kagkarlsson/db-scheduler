package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public interface CompletionHandler {

	void complete(ExecutionComplete executionComplete, Scheduler.ExecutionFinishedOperations executionFinishedOperations);


	class OnCompleteRemove implements CompletionHandler {

		@Override
		public void complete(ExecutionComplete executionComplete, Scheduler.ExecutionFinishedOperations executionFinishedOperations) {
			executionFinishedOperations.stop();
		}
	}

	class OnCompleteReschedule implements CompletionHandler {

		private static final Logger LOG = LoggerFactory.getLogger(OnCompleteReschedule.class);
		private final Schedule schedule;

		OnCompleteReschedule(Schedule schedule) {
			this.schedule = schedule;
		}

		@Override
		public void complete(ExecutionComplete executionComplete, Scheduler.ExecutionFinishedOperations executionFinishedOperations) {
			LocalDateTime nextExecution = schedule.getNextExecutionTime(executionComplete.getTimeDone());
			LOG.debug("Rescheduling task {} to {}", executionComplete.getExecution().taskInstance, nextExecution);
			executionFinishedOperations.reschedule(nextExecution);
		}
	}



}
