package com.kagkarlsson.scheduler;

import com.google.common.util.concurrent.ForwardingExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.kagkarlsson.scheduler.executors.CapacityLimitedExecutorService;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SchedulerTest {

	private Scheduler scheduler;
	private CustomHandler handler;
	private SettableClock clock;

	@Before
	public void setUp() {
		clock = new SettableClock();
		scheduler = new Scheduler(clock, new InMemoryTaskRespository(), new CapacityLimitedDirectExecutorService(), new Scheduler.FixedName("name"));
		handler = new CustomHandler();
	}

	@Test
	public void scheduler_should_execute_task_when_exactly_due() {
		OneTimeTask oneTimeTask = new OneTimeTask("OneTime", handler);

		LocalDateTime executionTime = clock.now().plusMinutes(1);
		scheduler.schedule(executionTime, oneTimeTask.instance("1"));

		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));

		clock.set(executionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(1));
	}

	@Test
	public void scheduler_should_execute_recurring_task_and_reschedule() {
		RecurringTask recurringTask = new RecurringTask("Recurring", Duration.ofHours(1), handler);

		scheduler.schedule(clock.now(), recurringTask.instance("single"));
		scheduler.executeDue();

		assertThat(handler.timesExecuted, is(1));

		LocalDateTime nextExecutionTime = clock.now().plusHours(1);
		clock.set(nextExecutionTime);
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(2));
	}

	@Test
	public void scheduler_should_stop_execution_when_executor_service_rejects() {
		scheduler = new Scheduler(clock, new InMemoryTaskRespository(), new CapacityLimitedDirectExecutorService(false), new Scheduler.FixedName("name"));
		OneTimeTask oneTimeTask = new OneTimeTask("OneTime", handler);

		scheduler.schedule(clock.now(), oneTimeTask.instance("1"));
		scheduler.executeDue();
		assertThat(handler.timesExecuted, is(0));
	}

	public static class CustomHandler implements Consumer<TaskInstance> {
		private int timesExecuted = 0;

		@Override
		public void accept(TaskInstance taskInstance) {
			this.timesExecuted++;
		}
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
