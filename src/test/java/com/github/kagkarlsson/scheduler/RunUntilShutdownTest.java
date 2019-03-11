package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RunUntilShutdownTest {

	private TimeLimitedRunnable runnable;
	private CountingWaiter countingWaiter;
	private RunUntilShutdown runUntilShutdown;
	private SchedulerState.SettableSchedulerState schedulerState;

	@Before
	public void setUp() {
		schedulerState = new SchedulerState.SettableSchedulerState();
		runnable = new TimeLimitedRunnable(2, schedulerState);
		countingWaiter = new CountingWaiter();
		runUntilShutdown = new RunUntilShutdown(runnable, countingWaiter,
				schedulerState, StatsRegistry.NOOP);
	}

	@Test(timeout = 1000)
	public void should_wait_on_ok_execution() {
		runUntilShutdown.run();
		assertThat(countingWaiter.counter, is(2));
	}

	@Test(timeout = 1000)
	public void should_wait_on_runtime_exception() {
		runnable.setAction(() -> {
			throw new RuntimeException();
		});
		runUntilShutdown.run();
		assertThat(countingWaiter.counter, is(2));
	}


	private static class TimeLimitedRunnable implements Runnable {
		private int times;
		private final SchedulerState.SettableSchedulerState schedulerState;
		private Runnable action;

		public TimeLimitedRunnable(int times, SchedulerState.SettableSchedulerState schedulerState) {
			this.times = times;
			this.schedulerState = schedulerState;
		}

		public void setAction(Runnable action) {
			this.action = action;
		}

		@Override
		public void run() {
			times--;
			if (times <= 0) {
				schedulerState.setIsShuttingDown();
			}
			if (action != null) action.run();
		}
	}

	private static class CountingWaiter extends Waiter {
		public int counter = 0;

		public CountingWaiter() {
			super(Duration.ofHours(1));
		}

		@Override
		public void doWait() throws InterruptedException {
			counter++;
		}

	}
}
