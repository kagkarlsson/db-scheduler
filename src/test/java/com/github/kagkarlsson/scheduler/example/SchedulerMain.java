package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.example.TasksMain.MyHourlyTask;
import com.github.kagkarlsson.scheduler.task.RecurringTask;
import com.github.kagkarlsson.scheduler.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class SchedulerMain {
	private static final Logger LOG = LoggerFactory.getLogger(SchedulerMain.class);

	private static void example(DataSource dataSource) {

		RecurringTask<Void> myRecurringTask = new MyHourlyTask();
		Task<TasksMain.MyTaskData> myAdhocTask = new TasksMain.MyTypedAdhocTask();

		final Scheduler scheduler = Scheduler
				.create(dataSource, myAdhocTask)
				.startTasks(myRecurringTask)
				.build();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOG.info("Received shutdown signal.");
				scheduler.stop();
			}
		});

		scheduler.start();
	}

	public static void main(String[] args) throws Throwable {
		try {
			final HsqlTestDatabaseRule hsqlRule = new HsqlTestDatabaseRule();
			hsqlRule.before();
			final DataSource dataSource = hsqlRule.getDataSource();

			example(dataSource);
		} catch (Exception e) {
			LOG.error("Error", e);
		}

	}

}
