package com.github.kagkarlsson.scheduler.functional;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlRule;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.StopSchedulerRule;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static com.github.kagkarlsson.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import static org.junit.Assert.assertEquals;

public class DeadExecutionTest {

	private SettableClock clock;

	@Rule
	public EmbeddedPostgresqlRule postgres = new EmbeddedPostgresqlRule(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);
	@Rule
	public Timeout timeout = new Timeout(5, TimeUnit.SECONDS);
	@Rule
	public StopSchedulerRule stopScheduler = new StopSchedulerRule();


	@Before
	public void setUp() {
		clock = new SettableClock();

	}

	@Test
	public void test_dead_execution() {
		CustomTask<Void> customTask = Tasks.custom("custom-a", Void.class)
				.execute((taskInstance, executionContext) -> new CompletionHandler<Void>() {
					@Override
					public void complete(ExecutionComplete executionComplete, ExecutionOperations<Void> executionOperations) {
						 //do nothing on complete, row will be left as-is in database
					}
				});

		TestableRegistry.Condition completedCondition = TestableRegistry.Conditions.completed(2);

		TestableRegistry registry = TestableRegistry.create().waitConditions(completedCondition).build();

		Scheduler scheduler = Scheduler.create(postgres.getDataSource(), customTask)
				.pollingInterval(Duration.ofMillis(100))
				.heartbeatInterval(Duration.ofMillis(100))
				.statsRegistry(registry)
				.build();
		stopScheduler.register(scheduler);

		scheduler.schedule(customTask.instance("1"), Instant.now());
		scheduler.start();
		completedCondition.waitFor();

		assertEquals(registry.getCount(SchedulerStatsEvent.DEAD_EXECUTION), 1);
	}

}
