package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.example.TasksMain.MyAdhocTask;
import com.github.kagkarlsson.scheduler.example.TasksMain.MyHourlyTask;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;

import static java.time.LocalDateTime.now;

public class SchedulerMain {
	private static final Logger LOG = LoggerFactory.getLogger(SchedulerMain.class);
	public static final String SINGLE_INSTANCE = "single";

	private static void example(DataSource dataSource) {

		Task myRecurringTask = new MyHourlyTask();
		Task myAdhocTask = new MyAdhocTask();

		final Scheduler scheduler = Scheduler
				.create(dataSource, new SchedulerName("myscheduler"), Lists.newArrayList(myRecurringTask, myAdhocTask))
				.build();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOG.info("Received shutdown signal.");
				scheduler.stop();
			}
		});

		scheduler.start();

		scheduler.scheduleForExecution(now(), myRecurringTask.instance(SINGLE_INSTANCE));
		scheduler.scheduleForExecution(now().plusSeconds(20), myAdhocTask.instance("1045"));

	}

	private static final ExecutionHandler LOGGING_EXECUTION_HANDLER = taskInstance -> {
		LOG.info("Executing task " + taskInstance.getTask().getName());
	};

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
