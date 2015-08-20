package com.kagkarlsson.scheduler;

import com.kagkarlsson.scheduler.task.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ClusterTest {

	@Rule
	public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();
	@Rule
	public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);

	@Before
	public void setUp() {
	}

	@Test
	public void test_concurrency() throws InterruptedException {
		final List<String> ids = IntStream.range(1, 1001).mapToObj(String::valueOf).collect(toList());
		final CountDownLatch completeAllIds = new CountDownLatch(ids.size());
		ResultRegisteringTask task = new ResultRegisteringTask("OneTime", (instance) -> {sleep(1);}, (id) -> completeAllIds.countDown());

		final SimpleStatsRegistry stats = new SimpleStatsRegistry();
		final Scheduler scheduler1 = createScheduler("scheduler1", task, stats);
		final Scheduler scheduler2 = createScheduler("scheduler2", task, stats);

		try {
			scheduler1.start();
			scheduler2.start();

			ids.forEach(id -> {
				scheduler1.addExecution(LocalDateTime.now(), task.instance(id));
			});

			completeAllIds.await();

			assertThat(task.failed.size(), is(0));
			assertThat(task.ok.size(), is(ids.size()));
			assertThat("Should contain no duplicates", new HashSet<>(task.ok).size(), is(ids.size()));
			assertThat(stats.unexpectedErrors.get(), is(0));

		} finally {
			scheduler1.stop();
			scheduler2.stop();
		}
	}

	private Scheduler createScheduler(String name, ResultRegisteringTask task, SimpleStatsRegistry stats) {
		return Scheduler.create(DB.getDataSource())
				.name(name)
				.pollingInterval(0, TimeUnit.MILLISECONDS)
				.detectDeadInterval(10, TimeUnit.MILLISECONDS)
				.addTask(task)
				.statsRegistry(stats)
				.build();
	}

	private void sleep(int millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static class ResultRegisteringTask extends OneTimeTask {

		private final Consumer<String> onComplete;
		private final List<String> ok = new ArrayList<>();
		private final List<String> failed = new ArrayList<>();

		public ResultRegisteringTask(String name, ExecutionHandler executionHandler, Consumer<String> onComplete) {
			super(name, executionHandler);
			this.onComplete = onComplete;
		}

		@Override
		public void complete(ExecutionComplete executionComplete, Scheduler.ExecutionFinishedOperations executionFinishedOperations) {
			final String instanceId = executionComplete.getExecution().taskInstance.getId();
			if (executionComplete.getResult() == ExecutionComplete.Result.OK) {
				ok.add(instanceId);
			} else {
				failed.add(instanceId);
			}
			super.complete(executionComplete, executionFinishedOperations);
			onComplete.accept(instanceId);
		}
	}

	private static class SimpleStatsRegistry implements StatsRegistry {
		public final AtomicInteger unexpectedErrors = new AtomicInteger(0);
		@Override
		public void registerUnexpectedError() {
			unexpectedErrors.incrementAndGet();
		}
	}


	public static final Consumer<TaskInstance> DO_NOTHING = (taskInstance -> {});

}
