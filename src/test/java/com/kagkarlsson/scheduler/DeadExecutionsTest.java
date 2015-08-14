package com.kagkarlsson.scheduler;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;

public class DeadExecutionsTest {

	@Rule
	public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();

	private Scheduler scheduler;
	private SettableClock settableClock;
	private OneTimeTask oneTimeTask;
	private JdbcTaskRepository jdbcTaskRepository;
	private TestLogger warnLogger;
	private TaskResolver taskResolver;

	@Before
	public void setUp() {
		settableClock = new SettableClock();
		oneTimeTask = new OneTimeTask("OneTime", new CustomHandler());

		taskResolver = new TaskResolver(new ArrayList<Task>(), TaskResolver.OnCannotResolve.FAIL_ON_UNRESOLVED);
		taskResolver.addTask(oneTimeTask);

		jdbcTaskRepository = new JdbcTaskRepository(DB.getDataSource(), taskResolver);
		warnLogger = new TestLogger();


		scheduler = new Scheduler(settableClock,
				jdbcTaskRepository,
				new SchedulerTest.CapacityLimitedDirectExecutorService(),
				new Scheduler.FixedName("test-scheduler"),
				new Scheduler.Waiter(0),
				new Scheduler.Waiter(100),
				warnLogger);

	}

	@Test
	public void scheduler_should_notify_detected_dead_executions() {
		final LocalDateTime now = settableClock.now();

		final Execution execution1 = new Execution(now.minusDays(1), oneTimeTask.instance("id1"));
		jdbcTaskRepository.createIfNotExists(execution1);

		final List<Execution> due = jdbcTaskRepository.getDue(now);
		assertThat(due, Matchers.hasSize(1));
		jdbcTaskRepository.pick(due.get(0));

		assertThat(jdbcTaskRepository.getDue(now), Matchers.hasSize(0));

		scheduler.detectDeadExecutions();

		assertThat(warnLogger.notifications, Matchers.hasSize(1));
	}

	public static class CustomHandler implements Consumer<TaskInstance> {
		private int timesExecuted = 0;

		@Override
		public void accept(TaskInstance taskInstance) {
			this.timesExecuted++;
		}
	}

	private static class TestLogger implements Consumer<String> {
		private static final Logger LOG = LoggerFactory.getLogger(TestLogger.class);
		List<String> notifications = new ArrayList<>();
		@Override
		public void accept(String s) {
			notifications.add(s);
			LOG.warn(s);
		}
	}
}
