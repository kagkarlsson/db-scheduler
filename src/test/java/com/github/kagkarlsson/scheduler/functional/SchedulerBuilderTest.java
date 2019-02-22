package com.github.kagkarlsson.scheduler.functional;

import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.ComposableTask;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.github.kagkarlsson.scheduler.JdbcTaskRepository.DEFAULT_TABLE_NAME;
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
		OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);
		TestableRegistry.Condition condition = TestableRegistry.Conditions.completed(10);
		Scheduler scheduler = Scheduler.create(postgres.getDataSource(), task)
				.pollingLimit(2)
				.pollingInterval(Duration.ofMinutes(1))
				.statsRegistry(TestableRegistry.create().waitCondition(condition).build())
				.build();
		stopScheduler.register(scheduler);

		IntStream.range(0, 10).forEach(i -> scheduler.schedule(task.instance(String.valueOf(i)), clock.now()));
		scheduler.start();

		condition.waitFor();
        
	}

}
