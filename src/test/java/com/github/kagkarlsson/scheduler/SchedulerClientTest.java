package com.github.kagkarlsson.scheduler;

import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.ArrayList;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.kagkarlsson.scheduler.task.OneTimeTask;
import com.google.common.util.concurrent.MoreExecutors;

public class SchedulerClientTest {

	@Rule
	public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();

	private Scheduler scheduler;
	private SettableClock settableClock;
	private OneTimeTask oneTimeTask;
	private JdbcTaskRepository jdbcTaskRepository;

	private TestTasks.CountingHandler onetimeTaskHandler;

	@Before
	public void setUp() {
		settableClock = new SettableClock();
		onetimeTaskHandler = new TestTasks.CountingHandler();
		oneTimeTask = TestTasks.oneTime("OneTime", onetimeTaskHandler);

		TaskResolver taskResolver = new TaskResolver(TaskResolver.OnCannotResolve.FAIL_ON_UNRESOLVED, oneTimeTask);

		jdbcTaskRepository = new JdbcTaskRepository(DB.getDataSource(), taskResolver, new SchedulerName.Fixed("scheduler1"));

		scheduler = new Scheduler(settableClock,
				jdbcTaskRepository,
				1,
				MoreExecutors.newDirectExecutorService(),
				new SchedulerName.Fixed("test-scheduler"),
				new Waiter(Duration.ZERO),
				Duration.ofMinutes(1),
				StatsRegistry.NOOP,
				new ArrayList<>());
		
	}
	
	@Test
	public void client_should_be_able_to_schedule_executions() {
		SchedulerClient client = SchedulerClient.Builder.create(DB.getDataSource()).build();
		client.scheduleForExecution(settableClock.now(), oneTimeTask.instance("1"));
		
		scheduler.executeDue();
		assertThat(onetimeTaskHandler.timesExecuted, CoreMatchers.is(1));
	}
	
}
