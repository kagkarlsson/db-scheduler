package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.task.*;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.github.kagkarlsson.scheduler.TestTasks.STRING_SERIALIZER;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;


public abstract class CompatibilityTest {

	@Rule
	public Timeout timeout = new Timeout(20, TimeUnit.SECONDS);

	private TestTasks.CountingHandler delayingHandler;
	private OneTimeTask<String> oneTime;
	private RecurringTask recurring;
	private TestTasks.SimpleStatsRegistry statsRegistry;
	private Scheduler scheduler;

	public abstract DataSource getDataSource();

	@Before
	public void setUp() {
		delayingHandler = new TestTasks.CountingHandler(Duration.ofMillis(200));
		oneTime = TestTasks.oneTimeWithType("oneTime", delayingHandler, STRING_SERIALIZER);
		recurring = TestTasks.recurring("recurring", FixedDelay.of(Duration.ofMillis(10)), delayingHandler);

		statsRegistry = new TestTasks.SimpleStatsRegistry();
		scheduler = Scheduler.create(getDataSource(), Lists.newArrayList(oneTime, recurring))
				.pollingInterval(Duration.ofMillis(10))
				.heartbeatInterval(Duration.ofMillis(100))
				.statsRegistry(statsRegistry)
				.build();
	}

	@After
	public void clearTables() {
		DbUtils.clearTables(getDataSource());
	}

	@Test
	public void test_compatibility() {
		scheduler.start();

		scheduler.schedule(Instant.now(), oneTime.instance("id1"));
		scheduler.schedule(Instant.now(), oneTime.instance("id1")); //duplicate
		scheduler.schedule(Instant.now(), recurring.instance("id1"));
		scheduler.schedule(Instant.now(), recurring.instance("id2"));
		scheduler.schedule(Instant.now(), recurring.instance("id3"));
		scheduler.schedule(Instant.now(), recurring.instance("id4"));

		sleep(Duration.ofSeconds(10));

		scheduler.stop();
		assertThat(statsRegistry.unexpectedErrors.get(), is(0));
		assertThat(delayingHandler.timesExecuted, greaterThan(10));
	}

	@Test
	public void test_jdbc_repository_compatibility() {
		doJDBCRepositoryCompatibilityTestUsingData(null);
	}

	@Test
	public void test_jdbc_repository_compatibility_with_data() {
		doJDBCRepositoryCompatibilityTestUsingData("my data");
	}

	private void doJDBCRepositoryCompatibilityTestUsingData(String data) {
		TaskResolver taskResolver = new TaskResolver(TaskResolver.OnCannotResolve.FAIL_ON_UNRESOLVED, new ArrayList<>());
		taskResolver.addTask(oneTime);

		final JdbcTaskRepository jdbcTaskRepository = new JdbcTaskRepository(getDataSource(), taskResolver, new SchedulerName.Fixed("scheduler1"));

		final Instant now = Instant.now();

		final TaskInstance taskInstance = oneTime.instance("id1", data);
		final Execution newExecution = new Execution(now, taskInstance);
		jdbcTaskRepository.createIfNotExists(newExecution);
		assertThat(jdbcTaskRepository.getExecution(taskInstance).get().getExecutionTime(), is(now));

		final List<Execution> due = jdbcTaskRepository.getDue(now);
		assertThat(due, hasSize(1));
		final Optional<Execution> pickedExecution = jdbcTaskRepository.pick(due.get(0), Instant.now());
		assertThat(pickedExecution.isPresent(), is(true));

		assertThat(jdbcTaskRepository.getDue(now), hasSize(0));

		jdbcTaskRepository.updateHeartbeat(pickedExecution.get(), now.plusSeconds(1));
		assertThat(jdbcTaskRepository.getOldExecutions(now.plus(Duration.ofDays(1))), hasSize(1));

		jdbcTaskRepository.reschedule(pickedExecution.get(), now.plusSeconds(1), now.minusSeconds(1), now.minusSeconds(1));
		assertThat(jdbcTaskRepository.getDue(now), hasSize(0));
		assertThat(jdbcTaskRepository.getDue(now.plus(Duration.ofMinutes(1))), hasSize(1));

		final Optional<Execution> rescheduled = jdbcTaskRepository.getExecution(taskInstance);
		assertThat(rescheduled.isPresent(), is(true));
		assertThat(rescheduled.get().lastHeartbeat, nullValue());
		assertThat(rescheduled.get().isPicked(), is(false));
		assertThat(rescheduled.get().pickedBy, nullValue());
		assertThat(rescheduled.get().taskInstance.getData(), is(data));
		jdbcTaskRepository.remove(rescheduled.get());
	}

	private void sleep(Duration duration) {
		try {
			Thread.sleep(duration.toMillis());
		} catch (InterruptedException e) {
			LoggerFactory.getLogger(CompatibilityTest.class).info("Interrupted");
		}
	}


}
