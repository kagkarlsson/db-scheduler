package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.ExecutionHandler;
import com.github.kagkarlsson.scheduler.task.FixedDelay;
import com.github.kagkarlsson.scheduler.task.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.RecurringTask;
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

		RecurringTask recurring1 = new RecurringTask("do_something", FixedDelay.of(Duration.ofSeconds(10)), LOGGING_EXECUTION_HANDLER);
		RecurringTask recurring2 = new RecurringTask("do_something_else", FixedDelay.of(Duration.ofSeconds(8)), LOGGING_EXECUTION_HANDLER);
		OneTimeTask onetime = new OneTimeTask("do_something_once", LOGGING_EXECUTION_HANDLER);

		final Scheduler scheduler = Scheduler
				.create(dataSource, new SchedulerName("myscheduler"), Lists.newArrayList(recurring1, recurring2, onetime))
				.build();

		scheduler.scheduleForExecution(now(), recurring1.instance(SINGLE_INSTANCE));
		scheduler.scheduleForExecution(now(), recurring2.instance(SINGLE_INSTANCE));
		scheduler.scheduleForExecution(now().plusSeconds(20), onetime.instance("1045"));

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOG.info("Received shutdown signal.");
				scheduler.stop();
			}
		});

		scheduler.start();
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
