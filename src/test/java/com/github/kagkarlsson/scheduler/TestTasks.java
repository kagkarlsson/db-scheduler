package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.ComposableTask.ExecutionHandlerWithExternalCompletion;
import com.github.kagkarlsson.scheduler.task.Task.Serializer;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TestTasks {

	public static final CompletionHandler REMOVE_ON_COMPLETE = new CompletionHandler.OnCompleteRemove();
	public static final ExecutionHandlerWithExternalCompletion<Void> DO_NOTHING = (taskInstance, executionContext) -> {};
	
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

	public static <T> OneTimeTask<T> oneTime(String name, Class<T> dataClass, ExecutionHandlerWithExternalCompletion<T> handler) {
		return new OneTimeTask<T>(name, dataClass) {
			@Override
			public void executeOnce(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
				handler.execute(taskInstance, executionContext);
			}
		};
	}

	public static <T> OneTimeTask<T> oneTimeWithType(String name, Class<T> dataClass, ExecutionHandlerWithExternalCompletion<T> handler, Serializer<T> serializer) {
		return new OneTimeTask<T>(name, dataClass, serializer) {
			@Override
			public void executeOnce(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
				handler.execute(taskInstance, executionContext);
			}
		};
	}

	public static RecurringTask recurring(String name, FixedDelay schedule, ExecutionHandlerWithExternalCompletion<Void> handler) {
		return new RecurringTask(name, schedule) {
			@Override
			public void executeRecurringly(TaskInstance<Void> taskInstance, ExecutionContext executionContext) {
				handler.execute(taskInstance, executionContext);
			}
		};
	}

	public static class ResultRegisteringCompletionHandler<T> implements CompletionHandler {
		CountDownLatch waitForNotify = new CountDownLatch(1);
		ExecutionComplete.Result result;
		Optional<Throwable> cause;

		@Override
		public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
			this.result = executionComplete.getResult();
			this.cause = executionComplete.getCause();
			executionOperations.stop();
			waitForNotify.countDown();
		}
	}

	public static class ResultRegisteringFailureHandler<T> implements FailureHandler {
		CountDownLatch waitForNotify = new CountDownLatch(1);
		ExecutionComplete.Result result;
		Optional<Throwable> cause;

		@Override
		public void onFailure(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
			this.result = executionComplete.getResult();
			this.cause = executionComplete.getCause();
			executionOperations.stop();
			waitForNotify.countDown();
		}
	}

	public static class CountingHandler<T> implements ExecutionHandlerWithExternalCompletion<T> {
		private final Duration wait;
		public int timesExecuted = 0;

		public CountingHandler() {
			wait = Duration.ofMillis(0);
		}
		public CountingHandler(Duration wait) {
			this.wait = wait;
		}

		@Override
		public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
			this.timesExecuted++;
			try {
				Thread.sleep(wait.toMillis());
			} catch (InterruptedException e) {
				LoggerFactory.getLogger(CountingHandler.class).info("Interrupted.");
			}
		}
	}

	public static class WaitingHandler<T> implements ExecutionHandlerWithExternalCompletion<T> {

		public final CountDownLatch waitForNotify;

		public WaitingHandler() {
			waitForNotify = new CountDownLatch(1);
		}

		@Override
		public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
			try {
				waitForNotify.await();
			} catch (InterruptedException e) {
				LoggerFactory.getLogger(WaitingHandler.class).info("Interrupted.");
			}
		}
	}
	
	public static class SleepingHandler<T> implements ExecutionHandlerWithExternalCompletion<T> {

		private int millis;

		public SleepingHandler(int seconds) {
			this.millis = seconds;
		}

		@Override
		public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
			try {
				Thread.sleep(millis);
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
