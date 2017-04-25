package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.Task.Serializer;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TestTasks {

	public static final ExecutionHandler DO_NOTHING = (taskInstance, executionContext) -> {};
	public static final Serializer<String> STRING_SERIALIZER = new Serializer<String>() {
		@Override
		public byte[] serialize(String data) {
			if(data == null) return null;
			return data.getBytes(StandardCharsets.UTF_8);
		}

		@Override
		public String deserialize(byte[] serializedData) {
			if(serializedData == null) return null;
			return new String(serializedData, StandardCharsets.UTF_8);
		}
	};

	public static OneTimeTask oneTime(String name, ExecutionHandler handler) {
		return new OneTimeTask(name) {
			@Override
			public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
				handler.execute(taskInstance, executionContext);
			}
		};
	}

	public static <T> OneTimeTask<T> oneTimeWithType(String name, ExecutionHandler<T> handler, Serializer<T> serializer) {
		return new OneTimeTask<T>(name, serializer) {
			@Override
			public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
				handler.execute(taskInstance, executionContext);
			}
		};
	}

	public static <T> RecurringTask<T> recurring(String name, FixedDelay schedule, ExecutionHandler<T> handler) {
		return new RecurringTask<T>(name, schedule) {
			@Override
			public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
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
