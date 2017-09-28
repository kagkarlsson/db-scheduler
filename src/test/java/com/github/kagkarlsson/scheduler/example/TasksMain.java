package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.HsqlTestDatabaseRule;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.*;
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
//			adhocTask(dataSource);
			simplerTaskDefinition(dataSource);
		} catch (Exception e) {
			LOG.error("Error", e);
		}
	}

	private static void recurringTask(DataSource dataSource) {

		TaskDescriptor<Void> taskDescriptor = new TaskDescriptor<>("my-recurring-task");

		final RecurringTask hourlyTask = new RecurringTask(taskDescriptor, FixedDelay.of(ofHours(1))) {
			@Override
			public void execute(TaskInstance<Void> taskInstance, ExecutionContext executionContext) {
				System.out.println("Executed!");
			}
		};

		final Scheduler scheduler = Scheduler
				.create(dataSource)
				.startTasks(hourlyTask)
				.threads(5)
				.build();

		// hourlyTask is automatically scheduled on startup if not already started (i.e. exists in the db)
		scheduler.start();
	}

	private static void adhocTask(DataSource dataSource) {

		TaskDescriptor<MyTaskData> TASK_DESCRIPTOR = new TaskDescriptor<>("my-adhoc-task", Task.Serializer.JAVA_SERIALIZER);

		final OneTimeTask myAdhocTask = new OneTimeTask<MyTaskData>(TASK_DESCRIPTOR) {

			@Override
			public void execute(TaskInstance<MyTaskData> taskInstance, ExecutionContext executionContext) {
				System.out.println(String.format("Executed! Custom data: [Id: %s], [secondary-id: %s]", taskInstance.getData().id, taskInstance.getData().secondaryId));
			}
		};

		final Scheduler scheduler = Scheduler
				.create(dataSource, myAdhocTask)
				.threads(5)
				.build();

		scheduler.start();

		// Schedule the task for execution a certain time in the future and optionally provide custom data for the execution
		scheduler.schedule(TASK_DESCRIPTOR.instance("1045", new MyTaskData(1001L, "custom-data")), Instant.now().plusSeconds(5));
	}

	public static class MyTaskData implements Serializable {
		public final long id;
		public final String secondaryId;

		public MyTaskData(long id, String secondaryId) {
			this.id = id;
			this.secondaryId = secondaryId;
		}
	}

	private static void simplerTaskDefinition(DataSource dataSource) {

		final RecurringTask myHourlyTask = ComposableTask.recurringTask("my-hourly-task", FixedDelay.of(ofHours(1)),
				() -> System.out.println("Executed!"));

		final TaskDescriptor MY_ONETIME_TASK = new TaskDescriptor<>("my-onetime-task", Task.Serializer.NO_SERIALIZER);

		final OneTimeTask oneTimeTask = ComposableTask.onetimeTask(MY_ONETIME_TASK,
				(taskInstance, context) -> System.out.println("One-time task with id "+taskInstance.getId()+" executed!"));

		final Scheduler scheduler = Scheduler
				.create(dataSource, oneTimeTask)
				.startTasks(myHourlyTask)
				.threads(5)
				.build();

		scheduler.start();


		scheduler.schedule(MY_ONETIME_TASK.instance("1001"), Instant.now().plus(Duration.ofSeconds(5)));
	}

}
