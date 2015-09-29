package com.github.kagkarlsson.scheduler;

import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ClusterTest {

	@Rule
	public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();
//	public EmbeddedPostgresqlRule DB = new EmbeddedPostgresqlRule(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);

	@Rule
	public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);

	@Test
	public void test_concurrency() throws InterruptedException {
		final List<String> ids = IntStream.range(1, 1001).mapToObj(String::valueOf).collect(toList());
		final CountDownLatch completeAllIds = new CountDownLatch(ids.size());
		ResultRegisteringTask task = new ResultRegisteringTask("OneTime", (instance) -> {sleep(1);}, (id) -> completeAllIds.countDown());

		final TestTasks.SimpleStatsRegistry stats = new TestTasks.SimpleStatsRegistry();
		final Scheduler scheduler1 = createScheduler("scheduler1", task, stats);
		final Scheduler scheduler2 = createScheduler("scheduler2", task, stats);

		try {
			scheduler1.start();
			scheduler2.start();

			ids.forEach(id -> {
				scheduler1.scheduleForExecution(LocalDateTime.now(), task.instance(id));
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

	private Scheduler createScheduler(String name, ResultRegisteringTask task, TestTasks.SimpleStatsRegistry stats) {
		return Scheduler.create(DB.getDataSource(), new SchedulerName(name), Lists.newArrayList(task))
				.pollingInterval(Duration.ofMillis(0))
				.heartbeatInterval(Duration.ofMillis(100))
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

		private final ExecutionHandler executionHandler;
		private final Consumer<String> onComplete;
		private final List<String> ok = Collections.synchronizedList(new ArrayList<>());
		private final List<String> failed = Collections.synchronizedList(new ArrayList<>());

		public ResultRegisteringTask(String name, ExecutionHandler executionHandler, Consumer<String> onComplete) {
			super(name);
			this.executionHandler = executionHandler;
			this.onComplete = onComplete;
		}

		@Override
		public void execute(TaskInstance taskInstance) {
			executionHandler.execute(taskInstance);
		}

		@Override
		public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
			final String instanceId = executionComplete.getExecution().taskInstance.getId();
			if (executionComplete.getResult() == ExecutionComplete.Result.OK) {
				ok.add(instanceId);
			} else {
				failed.add(instanceId);
			}
			super.complete(executionComplete, executionOperations);
			onComplete.accept(instanceId);
		}
	}

}
