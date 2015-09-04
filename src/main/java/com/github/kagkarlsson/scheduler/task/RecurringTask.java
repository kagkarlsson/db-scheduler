package com.github.kagkarlsson.scheduler.task;

public class RecurringTask extends Task {

	private final Schedule schedule;

	public RecurringTask(String name, Schedule schedule, ExecutionHandler executionHandler) {
		super(name, executionHandler, new CompletionHandler.OnCompleteReschedule(schedule));
		this.schedule = schedule;
	}

	public Schedule getSchedule() {
		return schedule;
	}

}
