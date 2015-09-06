package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.*;
import com.google.common.util.concurrent.MoreExecutors;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;

public class DeadExecutionsTest {

	@Rule
	public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();

	private Scheduler scheduler;
	private SettableClock settableClock;
	private OneTimeTask oneTimeTask;
	private JdbcTaskRepository jdbcTaskRepository;
	private NonCompletingTask nonCompleting;
	private TestTasks.CountingHandler nonCompletingExecutionHandler;
	private RescheduleDead deadExecutionHandler;

	@Before
	public void setUp() {
		settableClock = new SettableClock();
		oneTimeTask = new OneTimeTask("OneTime", TestTasks.DO_NOTHING);
		nonCompletingExecutionHandler = new TestTasks.CountingHandler();
		deadExecutionHandler = new RescheduleDead();
		nonCompleting = new NonCompletingTask("NonCompleting", nonCompletingExecutionHandler, deadExecutionHandler);

		TaskResolver taskResolver = new TaskResolver(new ArrayList<>(), TaskResolver.OnCannotResolve.FAIL_ON_UNRESOLVED);
		taskResolver.addTask(oneTimeTask);
		taskResolver.addTask(nonCompleting);

		jdbcTaskRepository = new JdbcTaskRepository(DB.getDataSource(), taskResolver, new SchedulerName("scheduler1"));

		scheduler = new Scheduler(settableClock,
				jdbcTaskRepository,
				1,
				MoreExecutors.newDirectExecutorService(),
				new SchedulerName("test-scheduler"),
				new Scheduler.Waiter(0),
				Duration.ofMinutes(1),
				StatsRegistry.NOOP);

	}

	@Test
	public void scheduler_should_handle_dead_executions() {
		final LocalDateTime now = settableClock.now();

		final TaskInstance taskInstance = oneTimeTask.instance("id1");
		final Execution execution1 = new Execution(now.minusDays(1), taskInstance);
		jdbcTaskRepository.createIfNotExists(execution1);

		final List<Execution> due = jdbcTaskRepository.getDue(now);
		assertThat(due, Matchers.hasSize(1));
		final Execution execution = due.get(0);
		jdbcTaskRepository.pick(execution, now);
		jdbcTaskRepository.updateHeartbeat(execution, now.minusHours(1));

		scheduler.detectDeadExecutions();

		final Optional<Execution> rescheduled = jdbcTaskRepository.getExecution(taskInstance);
		assertTrue(rescheduled.isPresent());
		assertThat(rescheduled.get().picked, is(false));
		assertThat(rescheduled.get().pickedBy, nullValue());

		assertThat(jdbcTaskRepository.getDue(LocalDateTime.now()), hasSize(1));
	}

	@Test
	public void scheduler_should_detect_dead_execution_that_never_updated_heartbeat() {
		final LocalDateTime now = LocalDateTime.now();
		settableClock.set(now.minusHours(1));
		final LocalDateTime oneHourAgo = settableClock.now();

		final TaskInstance taskInstance = nonCompleting.instance("id1");
		final Execution execution1 = new Execution(oneHourAgo, taskInstance);
		jdbcTaskRepository.createIfNotExists(execution1);

		scheduler.executeDue();
		assertThat(nonCompletingExecutionHandler.timesExecuted, is(1));

		scheduler.executeDue();
		assertThat(nonCompletingExecutionHandler.timesExecuted, is(1));

		settableClock.set(LocalDateTime.now());

		scheduler.detectDeadExecutions();
		assertThat(deadExecutionHandler.timesCalled, is(1));

		settableClock.set(LocalDateTime.now());

		scheduler.executeDue();
		assertThat(nonCompletingExecutionHandler.timesExecuted, is(2));
	}

	public static class NonCompletingTask extends Task {

		public NonCompletingTask(String name, ExecutionHandler handler, DeadExecutionHandler deadExecutionHandler) {
			super(name, handler, new DoNothingCompletionHandler(), deadExecutionHandler);
		}

	}

	public static class RescheduleDead extends DeadExecutionHandler.RescheduleDeadExecution {
		public int timesCalled = 0;

		@Override
		public void deadExecution(Execution execution, Scheduler.ExecutionOperations executionOperations) {
			timesCalled++;
			super.deadExecution(execution, executionOperations);
		}
	}

	public static class DoNothingCompletionHandler implements CompletionHandler {
		@Override
		public void complete(ExecutionComplete executionComplete, Scheduler.ExecutionOperations executionOperations) {
		}
	}
}
