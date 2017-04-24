package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.FixedDelay;
import com.github.kagkarlsson.scheduler.task.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.RecurringTask;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SchedulerTest {

	private Scheduler scheduler;
	private TestTasks.CountingHandler handler;
	private SettableClock clock;

	@Before
	public void setUp() {
		clock = new SettableClock();
		InMemoryTaskRespository taskRepository = new InMemoryTaskRespository(new SchedulerName.Fixed("scheduler1"));
		scheduler = new Scheduler(clock, taskRepository, 1, MoreExecutors.newDirectExecutorService(), new SchedulerName.Fixed("name"), new Waiter(Duration.ZERO), Duration.ofSeconds(1), StatsRegistry.NOOP, new ArrayList<>());
		handler = new TestTasks.CountingHandler();
	}

	@Test
	public void scheduler_should_execute_task_when_exactly_due() {
		OneTimeTask oneTimeTask = TestTasks.oneTime("OneTime", handler);

		Instant executionTime = clock.now().plus(Duration.ofMinutes(1));
		scheduler.schedule(executionTime, oneTimeTask.instance("1"));

		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));

		clock.set(executionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(1));
	}

	@Test
	public void scheduler_should_execute_rescheduled_task_when_exactly_due() {
		String taskName = "OneTime";
		OneTimeTask oneTimeTask = TestTasks.oneTime(taskName, handler);

		Instant executionTime = clock.now().plus(Duration.ofMinutes(1));
		String instanceId = "1";
		scheduler.schedule(executionTime, oneTimeTask.instance(instanceId));
		Instant reScheduledExecutionTime = clock.now().plus(Duration.ofMinutes(2));
		scheduler.reschedule(taskName, instanceId, reScheduledExecutionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));

		clock.set(executionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));

		clock.set(reScheduledExecutionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(1));
	}

	@Test
	public void scheduler_should_not_execute_canceled_tasks() {
		String taskName = "OneTime";
		OneTimeTask oneTimeTask = TestTasks.oneTime(taskName, handler);

		Instant executionTime = clock.now().plus(Duration.ofMinutes(1));
		String instanceId = "1";
		scheduler.schedule(executionTime, oneTimeTask.instance(instanceId));
		scheduler.cancel(taskName, instanceId);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));

		clock.set(executionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));
	}

	@Test
	public void scheduler_should_execute_recurring_task_and_reschedule() {
		RecurringTask recurringTask = TestTasks.recurring("Recurring", FixedDelay.of(Duration.ofHours(1)), handler);

		scheduler.schedule(clock.now(), recurringTask.instance("single"));
		scheduler.executeDue();

		assertThat(handler.timesExecuted, is(1));

		Instant nextExecutionTime = clock.now().plus(Duration.ofHours(1));
		clock.set(nextExecutionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(2));
	}

	@Test
	public void scheduler_should_stop_execution_when_executor_service_rejects() throws InterruptedException {
		scheduler = new Scheduler(clock, new InMemoryTaskRespository(new SchedulerName.Fixed("scheduler1")), 1, MoreExecutors.newDirectExecutorService(), new SchedulerName.Fixed("name"), new Waiter(Duration.ZERO), Duration.ofMinutes(1), StatsRegistry.NOOP, new ArrayList<>());
		scheduler.executorsSemaphore.acquire();
		OneTimeTask oneTimeTask = TestTasks.oneTime("OneTime", handler);

		scheduler.schedule(clock.now(), oneTimeTask.instance("1"));
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));
	}

	@Test
	public void scheduler_should_track_duration() {
		scheduler = new Scheduler(clock, new InMemoryTaskRespository(new SchedulerName.Fixed("scheduler1")), 1, Executors.newSingleThreadExecutor(), new SchedulerName.Fixed("name"), new Waiter(Duration.ZERO), Duration.ofMinutes(1), StatsRegistry.NOOP, new ArrayList<>());
		OneTimeTask oneTimeTask = TestTasks.oneTime("OneTime", new TestTasks.WaitingHandler());

		scheduler.schedule(clock.now(), oneTimeTask.instance("1"));
		scheduler.executeDue();

		assertThat(scheduler.getCurrentlyExecuting(), hasSize(1));
		clock.set(clock.now.plus(Duration.ofMinutes(1)));

		assertThat(scheduler.getCurrentlyExecuting().get(0).getDuration(), is(Duration.ofMinutes(1)));
	}


}
