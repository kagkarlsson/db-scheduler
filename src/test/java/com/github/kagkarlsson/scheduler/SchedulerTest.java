package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.FixedDelay;
import com.github.kagkarlsson.scheduler.task.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.RecurringTask;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SchedulerTest {

	private Scheduler scheduler;
	private TestTasks.CountingHandler handler;
	private SettableClock clock;

	@Before
	public void setUp() {
		clock = new SettableClock();
		InMemoryTaskRespository taskRepository = new InMemoryTaskRespository(new SchedulerName("scheduler1"));
		scheduler = new Scheduler(clock, taskRepository, 1, MoreExecutors.newDirectExecutorService(), new SchedulerName("name"), new Scheduler.Waiter(0), Duration.ofSeconds(1), StatsRegistry.NOOP);
		handler = new TestTasks.CountingHandler();
	}

	@Test
	public void scheduler_should_execute_task_when_exactly_due() {
		OneTimeTask oneTimeTask = new OneTimeTask("OneTime", handler);

		LocalDateTime executionTime = clock.now().plusMinutes(1);
		scheduler.scheduleForExecution(executionTime, oneTimeTask.instance("1"));

		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));

		clock.set(executionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(1));
	}

	@Test
	public void scheduler_should_execute_recurring_task_and_reschedule() {
		RecurringTask recurringTask = new RecurringTask("Recurring", FixedDelay.of(Duration.ofHours(1)), handler);

		scheduler.scheduleForExecution(clock.now(), recurringTask.instance("single"));
		scheduler.executeDue();

		assertThat(handler.timesExecuted, is(1));

		LocalDateTime nextExecutionTime = clock.now().plusHours(1);
		clock.set(nextExecutionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(2));
	}

	@Test
	public void scheduler_should_stop_execution_when_executor_service_rejects() throws InterruptedException {
		scheduler = new Scheduler(clock, new InMemoryTaskRespository(new SchedulerName("scheduler1")), 1, MoreExecutors.newDirectExecutorService(), new SchedulerName("name"), new Scheduler.Waiter(0), Duration.ofMinutes(1), StatsRegistry.NOOP);
		scheduler.executorsSemaphore.acquire();
		OneTimeTask oneTimeTask = new OneTimeTask("OneTime", handler);

		scheduler.scheduleForExecution(clock.now(), oneTimeTask.instance("1"));
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));
	}

}
