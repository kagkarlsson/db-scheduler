package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.CompletionHandler.OnCompleteReschedule;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler.RescheduleDeadExecution;

public class RecurringTask extends Task {

	private final Schedule schedule;

	public RecurringTask(String name, Schedule schedule, ExecutionHandler executionHandler) {
		super(name, executionHandler, new OnCompleteReschedule(schedule), new RescheduleDeadExecution());
		this.schedule = schedule;
	}

	public Schedule getSchedule() {
		return schedule;
	}

}
