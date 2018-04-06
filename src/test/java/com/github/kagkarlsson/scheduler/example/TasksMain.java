package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.HsqlTestDatabaseRule;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.ComposableTask;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

import static java.time.Duration.ofHours;

public class TasksMain {
	private static final Logger LOG = LoggerFactory.getLogger(TasksMain.class);

	public static void main(String[] args) throws Throwable {
		try {
			final HsqlTestDatabaseRule hsqlRule = new HsqlTestDatabaseRule();
			hsqlRule.before();
			final DataSource dataSource = hsqlRule.getDataSource();

//			recurringTask(dataSource);
			adhocTask(dataSource);
		} catch (Exception e) {
			LOG.error("Error", e);
		}
	}

	private static void recurringTask(DataSource dataSource) {

		RecurringTask<Void> hourlyTask = Tasks.recurring("my-hourly-task", FixedDelay.ofHours(1))
				.execute((inst, ctx) -> {
					System.out.println("Executed!");
				});

		final Scheduler scheduler = Scheduler
				.create(dataSource)
				.startTasks(hourlyTask)
				.threads(5)
				.build();

		// hourlyTask is automatically scheduled on startup if not already started (i.e. exists in the db)
		scheduler.start();
	}

	private static void adhocTask(DataSource dataSource) {

		OneTimeTask<MyTaskData> myAdhocTask = Tasks.oneTime("my-typed-adhoc-task", MyTaskData.class)
				.execute((inst, ctx) -> {
					System.out.println("Executed! Custom data, Id: " + inst.getData().id);
				});

		final Scheduler scheduler = Scheduler
				.create(dataSource, myAdhocTask)
				.threads(5)
				.build();

		scheduler.start();

		// Schedule the task for execution a certain time in the future and optionally provide custom data for the execution
		scheduler.schedule(myAdhocTask.instance("1045", new MyTaskData(1001L)), Instant.now().plusSeconds(5));
	}

	public static class MyTaskData implements Serializable {
		public final long id;

		public MyTaskData(long id) {
			this.id = id;
		}
	}

}
