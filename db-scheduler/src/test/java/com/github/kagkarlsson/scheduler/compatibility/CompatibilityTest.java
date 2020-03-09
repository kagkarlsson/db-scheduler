package com.github.kagkarlsson.scheduler.compatibility;

import static com.github.kagkarlsson.scheduler.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.TestTasks.DoNothingHandler;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;


@SuppressWarnings("ConstantConditions")
public abstract class CompatibilityTest {

	private static final int POLLING_LIMIT = 10_000;

	@RegisterExtension
	public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

	private TestTasks.CountingHandler<String> delayingHandlerOneTime;
	private TestTasks.CountingHandler<Void> delayingHandlerRecurring;
	private OneTimeTask<String> oneTime;
	private RecurringTask<Void> recurring;
	private RecurringTask<Integer> recurringWithData;
	private TestTasks.SimpleStatsRegistry statsRegistry;
	private Scheduler scheduler;

	public abstract DataSource getDataSource();

	@BeforeEach
	public void setUp() {
		delayingHandlerOneTime = new TestTasks.CountingHandler<>(Duration.ofMillis(200));
		delayingHandlerRecurring = new TestTasks.CountingHandler<>(Duration.ofMillis(200));

		oneTime = TestTasks.oneTimeWithType("oneTime", String.class, delayingHandlerOneTime);
		recurring = TestTasks.recurring("recurring", FixedDelay.of(Duration.ofMillis(10)), delayingHandlerRecurring);
		recurringWithData = TestTasks.recurringWithData("recurringWithData", Integer.class, 0, FixedDelay.of(Duration.ofMillis(10)), new DoNothingHandler<>());

		statsRegistry = new TestTasks.SimpleStatsRegistry();
		scheduler = Scheduler.create(getDataSource(), Lists.newArrayList(oneTime, recurring))
				.pollingInterval(Duration.ofMillis(10))
				.heartbeatInterval(Duration.ofMillis(100))
				.statsRegistry(statsRegistry)
				.build();
		stopScheduler.register(scheduler);
	}

	@AfterEach
	public void clearTables() {
	    assertTimeout(Duration.ofSeconds(20), () ->
            DbUtils.clearTables(getDataSource())
        );
	}

	@Test
	public void test_compatibility() {
        assertTimeout(Duration.ofSeconds(20), () -> {
            scheduler.start();

            scheduler.schedule(oneTime.instance("id1"), Instant.now());
            scheduler.schedule(oneTime.instance("id1"), Instant.now()); //duplicate
            scheduler.schedule(recurring.instance("id1"), Instant.now());
            scheduler.schedule(recurring.instance("id2"), Instant.now());
            scheduler.schedule(recurring.instance("id3"), Instant.now());
            scheduler.schedule(recurring.instance("id4"), Instant.now());

            sleep(Duration.ofSeconds(10));

            scheduler.stop();
            assertThat(statsRegistry.unexpectedErrors.get(), is(0));
            assertThat(delayingHandlerRecurring.timesExecuted, greaterThan(10));
            assertThat(delayingHandlerOneTime.timesExecuted, is(1));
        });
	}

	@Test
	public void test_jdbc_repository_compatibility() {
        assertTimeout(Duration.ofSeconds(20), () -> {
            doJDBCRepositoryCompatibilityTestUsingData(null);
        });
	}

	@Test
	public void test_jdbc_repository_compatibility_with_data() {
        assertTimeout(Duration.ofSeconds(20), () -> {
            doJDBCRepositoryCompatibilityTestUsingData("my data");
        });
	}

	private void doJDBCRepositoryCompatibilityTestUsingData(String data) {
		TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, new ArrayList<>());
		taskResolver.addTask(oneTime);

		final JdbcTaskRepository jdbcTaskRepository = new JdbcTaskRepository(getDataSource(), DEFAULT_TABLE_NAME, taskResolver, new SchedulerName.Fixed("scheduler1"));

		final Instant now = Instant.now();

		final TaskInstance<String> taskInstance = oneTime.instance("id1", data);
		final Execution newExecution = new Execution(now, taskInstance);
		jdbcTaskRepository.createIfNotExists(newExecution);
		assertThat((jdbcTaskRepository.getExecution(taskInstance)).get().getExecutionTime(), is(now));

		final List<Execution> due = jdbcTaskRepository.getDue(now, POLLING_LIMIT);
		assertThat(due, hasSize(1));
		final Optional<Execution> pickedExecution = jdbcTaskRepository.pick(due.get(0), Instant.now());
		assertThat(pickedExecution.isPresent(), is(true));

		assertThat(jdbcTaskRepository.getDue(now, POLLING_LIMIT), hasSize(0));

		jdbcTaskRepository.updateHeartbeat(pickedExecution.get(), now.plusSeconds(1));
		assertThat(jdbcTaskRepository.getDeadExecutions(now.plus(Duration.ofDays(1))), hasSize(1));

		jdbcTaskRepository.reschedule(pickedExecution.get(), now.plusSeconds(1), now.minusSeconds(1), now.minusSeconds(1), 0);
		assertThat(jdbcTaskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
		assertThat(jdbcTaskRepository.getDue(now.plus(Duration.ofMinutes(1)), POLLING_LIMIT), hasSize(1));

		final Optional<Execution> rescheduled = jdbcTaskRepository.getExecution(taskInstance);
		assertThat(rescheduled.isPresent(), is(true));
		assertThat(rescheduled.get().lastHeartbeat, nullValue());
		assertThat(rescheduled.get().isPicked(), is(false));
		assertThat(rescheduled.get().pickedBy, nullValue());
		assertThat(rescheduled.get().taskInstance.getData(), is(data));
		jdbcTaskRepository.remove(rescheduled.get());
	}

	@Test
	public void test_jdbc_repository_compatibility_set_data() {
		TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, new ArrayList<>());
		taskResolver.addTask(recurringWithData);

		final JdbcTaskRepository jdbcTaskRepository = new JdbcTaskRepository(getDataSource(), DEFAULT_TABLE_NAME, taskResolver, new SchedulerName.Fixed("scheduler1"));

		final Instant now = Instant.now();

		final TaskInstance<Integer> taskInstance = recurringWithData.instance("id1", 1);
		final Execution newExecution = new Execution(now, taskInstance);

		jdbcTaskRepository.createIfNotExists(newExecution);

		Execution round1 = jdbcTaskRepository.getExecution(taskInstance).get();
		assertEquals(round1.taskInstance.getData(), 1);
		jdbcTaskRepository.reschedule(round1, now.plusSeconds(1), 2, now.minusSeconds(1), now.minusSeconds(1), 0);

		Execution round2 = jdbcTaskRepository.getExecution(taskInstance).get();
		assertEquals(round2.taskInstance.getData(), 2);

		jdbcTaskRepository.reschedule(round2, now.plusSeconds(2), null, now.minusSeconds(2), now.minusSeconds(2), 0);
		Execution round3 = jdbcTaskRepository.getExecution(taskInstance).get();
		assertNull(round3.taskInstance.getData());
	}


	private void sleep(Duration duration) {
		try {
			Thread.sleep(duration.toMillis());
		} catch (InterruptedException e) {
			LoggerFactory.getLogger(CompatibilityTest.class).info("Interrupted");
		}
	}


}
