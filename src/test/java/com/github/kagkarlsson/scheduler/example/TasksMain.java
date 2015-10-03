package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.HsqlTestDatabaseRule;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.LocalDateTime;

public class TasksMain {
	private static final Logger LOG = LoggerFactory.getLogger(TasksMain.class);

	public static void main(String[] args) throws Throwable {
		try {
			final HsqlTestDatabaseRule hsqlRule = new HsqlTestDatabaseRule();
			hsqlRule.before();
			final DataSource dataSource = hsqlRule.getDataSource();

			recurringTask(dataSource);
		} catch (Exception e) {
			LOG.error("Error", e);
		}
	}

	private static void recurringTask(DataSource dataSource) {

		final MyHourlyTask hourlyTask = new MyHourlyTask();

		final Scheduler scheduler = Scheduler
				.create(dataSource, hourlyTask)
				.startTasks(hourlyTask)
				.threads(5)
				.build();

		// hourlyTask is automatically scheduled on startup if not already started (i.e. in the db)
		scheduler.start();
	}

	public static class MyHourlyTask extends RecurringTask {

		public MyHourlyTask() {
			super("my-hourly-task", FixedDelay.of(Duration.ofHours(1)));
		}

		@Override
		public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
			System.out.println("Executed!");
		}
	}

	private static void adhocExecution(DataSource dataSource) {

		final MyAdhocTask myAdhocTask = new MyAdhocTask();

		final Scheduler scheduler = Scheduler
				.create(dataSource, myAdhocTask)
				.threads(5)
				.build();

		scheduler.start();

		// Schedule the task for execution a certain time in the future
		scheduler.scheduleForExecution(LocalDateTime.now().plusMinutes(5), myAdhocTask.instance("1045"));
	}

	public static class MyAdhocTask extends OneTimeTask {

		public MyAdhocTask() {
			super("my-adhoc-task");
		}

		@Override
		public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
			System.out.println("Executed!");
		}
	}

}
