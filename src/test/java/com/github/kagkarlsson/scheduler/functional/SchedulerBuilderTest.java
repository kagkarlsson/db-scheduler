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

public class SchedulerBuilderTest {

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
	public void test_execute_until_none_left() {
		Instant now = Instant.now();
		OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);
		TestableRegistry.Condition condition = TestableRegistry.Conditions.completed(10);
		TestableRegistry registry = TestableRegistry.create().logEvents().waitConditions(condition).build();

		Scheduler scheduler = Scheduler.create(postgres.getDataSource(), task)
				.pollingLimit(2)
				.threads(2)
				.pollingInterval(Duration.ofMinutes(1))
				.statsRegistry(registry)
				.build();
		stopScheduler.register(scheduler);

		IntStream.range(0, 10).forEach(i -> scheduler.schedule(task.instance(String.valueOf(i)), clock.now()));

		scheduler.start();
		condition.waitFor();

		List<ExecutionComplete> completed = registry.getCompleted();
		assertThat(completed, hasSize(10));
		completed.stream().forEach(e -> {
			assertThat(e.getResult(), is(ExecutionComplete.Result.OK));
			Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
			assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
		});
		registry.assertNoFailures();
	}

	@Test
	public void test_immediate_execution() {
		Instant now = Instant.now();
		OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);
		TestableRegistry.Condition completedCondition = TestableRegistry.Conditions.completed(1);
		TestableRegistry.Condition executeDueCondition = TestableRegistry.Conditions.ranExecuteDue(1);

		TestableRegistry registry = TestableRegistry.create().waitConditions(executeDueCondition, completedCondition).build();

		Scheduler scheduler = Scheduler.create(postgres.getDataSource(), task)
				.pollingInterval(Duration.ofMinutes(1))
				.enableImmediateExecution()
				.statsRegistry(registry)
				.build();
		stopScheduler.register(scheduler);

		scheduler.start();
		executeDueCondition.waitFor();

		scheduler.schedule(task.instance("1"), clock.now());
		completedCondition.waitFor();

		List<ExecutionComplete> completed = registry.getCompleted();
		assertThat(completed, hasSize(1));
		completed.stream().forEach(e -> {
			assertThat(e.getResult(), is(ExecutionComplete.Result.OK));
			Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
			assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
		});
		registry.assertNoFailures();
	}

}
