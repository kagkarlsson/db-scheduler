package com.github.kagkarlsson.scheduler.functional;

import co.unruly.matchers.TimeMatchers;
import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ExecutorPoolTest {

	private SettableClock clock;

	@Rule
	public EmbeddedPostgresqlRule postgres = new EmbeddedPostgresqlRule(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);
	@Rule
	public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);
	@Rule
	public StopSchedulerRule stopScheduler = new StopSchedulerRule();


	@Before
	public void setUp() {
		clock = new SettableClock();

	}

	@Test
	public void test_execute_until_none_left_happy() {
		testExecuteUntilNoneLeft(2, 2, 20);
	}

	@Test
	public void test_execute_until_none_left_low_polling_limit() {
		testExecuteUntilNoneLeft(2, 10, 20);
	}

	@Test
	public void test_execute_until_none_left_high_volume() {
		testExecuteUntilNoneLeft(12, 4, 200);
	}


	private void testExecuteUntilNoneLeft(int pollingLimit, int threads, int executionsToRun) {
		Instant now = Instant.now();
		OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);
		TestableRegistry.Condition condition = TestableRegistry.Conditions.completed(executionsToRun);
		TestableRegistry registry = TestableRegistry.create().waitConditions(condition).build();

		Scheduler scheduler = Scheduler.create(postgres.getDataSource(), task)
				.pollingLimit(pollingLimit)
				.threads(threads)
				.pollingInterval(Duration.ofMinutes(1))
				.statsRegistry(registry)
				.build();
		stopScheduler.register(scheduler);

		IntStream.range(0, executionsToRun).forEach(i -> scheduler.schedule(task.instance(String.valueOf(i)), clock.now()));

		scheduler.start();
		condition.waitFor();

		List<ExecutionComplete> completed = registry.getCompleted();
		assertThat(completed, hasSize(executionsToRun));
		completed.stream().forEach(e -> {
			assertThat(e.getResult(), is(ExecutionComplete.Result.OK));
			Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
			assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
		});
		registry.assertNoFailures();
	}

}
