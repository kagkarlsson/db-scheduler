package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.HsqlTestDatabaseRule;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;

public class SchedulerMain {
	private static final Logger LOG = LoggerFactory.getLogger(SchedulerMain.class);

	private static void example(DataSource dataSource) {

		// recurring with no data
		RecurringTask<Void> recurring1 = Tasks.recurring("recurring_no_data", FixedDelay.of(Duration.ofSeconds(5)))
				.onFailureReschedule()   // default
				.onDeadExecutionRevive() // default
				.execute((taskInstance, executionContext) -> {
					System.out.println("Executing " + taskInstance.getTaskAndInstance());
				});

		// recurring with contant data
		RecurringTask<Integer> recurring2 = Tasks.recurring("recurring_constant_data", FixedDelay.of(Duration.ofSeconds(7)), Integer.class)
				.initialData(1)
				.onFailureReschedule()   // default
				.onDeadExecutionRevive() // default
				.execute((taskInstance, executionContext) -> {
					System.out.println("Executing " + taskInstance.getTaskAndInstance() + " , data: " + taskInstance.getData());
				});

		// recurring with changing data
		Schedule custom1Schedule = FixedDelay.of(Duration.ofSeconds(4));
		CustomTask<Integer> custom1 = Tasks.custom("recurring_changing_data", Integer.class)
				.scheduleOnStartup("instance1", 1)
				.onFailureReschedule(custom1Schedule)  // default
				.onDeadExecutionRevive()               // default
				.execute((taskInstance, executionContext) -> {

					System.out.println("Executing " + taskInstance.getTaskAndInstance() + " , data: " + taskInstance.getData());
					return (executionComplete, executionOperations) -> {
						Instant nextExecutionTime = custom1Schedule.getNextExecutionTime(executionComplete);
						int newData = taskInstance.getData() + 1;
						executionOperations.reschedule(executionComplete, nextExecutionTime, newData);
					};
				});

		// one-time with no data
		OneTimeTask<Void> onetime1 = Tasks.oneTime("onetime_no_data")
				.onDeadExecutionRevive()  // default
				.onFailureRetryLater()    // default
				.execute((TaskInstance<Void> taskInstance, ExecutionContext executionContext) -> {
					System.out.println("Executing " + taskInstance.getTaskAndInstance());
				});

		// one-time with data
		OneTimeTask<Integer> onetime2 = Tasks.oneTime("onetime_withdata", Integer.class)
				.onFailureRetryLater()    // default
				.execute((TaskInstance<Integer> taskInstance, ExecutionContext executionContext) -> {
					System.out.println("Executing " + taskInstance.getTaskAndInstance() + " , data: " + taskInstance.getData());
				});


		final Scheduler scheduler = Scheduler
				.create(dataSource, onetime1, onetime2)
				.startTasks(recurring1, recurring2, custom1)
				.build();

		scheduler.schedule(onetime1.instance("onetime1"), Instant.now());
		scheduler.schedule(onetime2.instance("onetime2", 100), Instant.now().plusSeconds(3));

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOG.info("Received shutdown signal.");
				scheduler.stop();
			}
		});

		scheduler.start();

		scheduler.schedule(onetime2.instance("onetime3", 100), Instant.now());
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
