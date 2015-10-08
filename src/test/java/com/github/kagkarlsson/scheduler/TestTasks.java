package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.*;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TestTasks {

	public static final ExecutionHandler DO_NOTHING = (taskInstance, executionContext) -> {};

	public static OneTimeTask oneTime(String name, ExecutionHandler handler) {
		return new OneTimeTask(name) {
			@Override
			public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
				handler.execute(taskInstance, executionContext);
			}
		};
	}

	public static RecurringTask recurring(String name, FixedDelay schedule, ExecutionHandler handler) {
		return new RecurringTask(name, schedule) {
			@Override
			public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
				handler.execute(taskInstance, executionContext);
			}
		};
	}

	public static class CountingHandler implements ExecutionHandler {
		private final Duration wait;
		public int timesExecuted = 0;

		public CountingHandler() {
			wait = Duration.ofMillis(0);
		}
		public CountingHandler(Duration wait) {
			this.wait = wait;
		}

		@Override
		public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
			this.timesExecuted++;
			try {
				Thread.sleep(wait.toMillis());
			} catch (InterruptedException e) {
				LoggerFactory.getLogger(CountingHandler.class).info("Interrupted.");
			}
		}
	}

	public static class WaitingHandler implements ExecutionHandler {

		private final CountDownLatch waitForNotify;

		public WaitingHandler() {
			waitForNotify = new CountDownLatch(1);
		}

		@Override
		public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
			try {
				waitForNotify.await();
			} catch (InterruptedException e) {
				LoggerFactory.getLogger(WaitingHandler.class).info("Interrupted.");
			}
		}
	}

	public static class SimpleStatsRegistry implements StatsRegistry {
		public final AtomicInteger unexpectedErrors = new AtomicInteger(0);
		@Override
		public void registerUnexpectedError() {
			unexpectedErrors.incrementAndGet();
		}
	}

}
