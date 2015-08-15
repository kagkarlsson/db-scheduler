package com.kagkarlsson.scheduler;

import com.google.common.util.concurrent.ForwardingExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.kagkarlsson.scheduler.executors.CapacityLimitedExecutorService;
import com.kagkarlsson.scheduler.task.FixedDelay;
import com.kagkarlsson.scheduler.task.OneTimeTask;
import com.kagkarlsson.scheduler.task.RecurringTask;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SchedulerTest {

	private Scheduler scheduler;
	private TestTasks.CountingHandler handler;
	private SettableClock clock;
	private InMemoryTaskRespository taskRepository;

	@Before
	public void setUp() {
		clock = new SettableClock();
		taskRepository = new InMemoryTaskRespository();
		scheduler = new Scheduler(clock, taskRepository, new CapacityLimitedDirectExecutorService(), new Scheduler.FixedName("name"), new Scheduler.Waiter(0), new Scheduler.Waiter(100), Scheduler.WARN_LOGGER);
		handler = new TestTasks.CountingHandler();
	}

	@Test
	public void scheduler_should_execute_task_when_exactly_due() {
		OneTimeTask oneTimeTask = new OneTimeTask("OneTime", handler);

		LocalDateTime executionTime = clock.now().plusMinutes(1);
		scheduler.addExecution(executionTime, oneTimeTask.instance("1"));

		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));

		clock.set(executionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(1));
	}

	@Test
	public void scheduler_should_execute_recurring_task_and_reschedule() {
		RecurringTask recurringTask = new RecurringTask("Recurring", FixedDelay.of(Duration.ofHours(1)), handler);

		scheduler.addExecution(clock.now(), recurringTask.instance("single"));
		scheduler.executeDue();

		assertThat(handler.timesExecuted, is(1));

		LocalDateTime nextExecutionTime = clock.now().plusHours(1);
		clock.set(nextExecutionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(2));
	}

	@Test
	public void scheduler_should_stop_execution_when_executor_service_rejects() {
		scheduler = new Scheduler(clock, new InMemoryTaskRespository(), new CapacityLimitedDirectExecutorService(false), new Scheduler.FixedName("name"), new Scheduler.Waiter(0), new Scheduler.Waiter(100), Scheduler.WARN_LOGGER);
		OneTimeTask oneTimeTask = new OneTimeTask("OneTime", handler);

		scheduler.addExecution(clock.now(), oneTimeTask.instance("1"));
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));
	}

	public static class CapacityLimitedDirectExecutorService extends ForwardingExecutorService implements CapacityLimitedExecutorService {

		private final ExecutorService delegate;
		private final boolean hasFreeExecutor;

		public CapacityLimitedDirectExecutorService() {
			this(true);
		}
		public CapacityLimitedDirectExecutorService(boolean hasFreeExecutor) {
			this.hasFreeExecutor = hasFreeExecutor;
			this.delegate = MoreExecutors.newDirectExecutorService();
		}

		@Override
		public boolean hasFreeExecutor() {
			return hasFreeExecutor;
		}

		@Override
		protected ExecutorService delegate() {
			return delegate;
		}
	}
}
