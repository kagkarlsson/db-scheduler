package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.*;
import com.google.common.util.concurrent.MoreExecutors;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
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
	private JdbcTaskRepository<Object> jdbcTaskRepository;
	private NonCompletingTask nonCompleting;
	private TestTasks.CountingHandler nonCompletingExecutionHandler;
	private RescheduleDead deadExecutionHandler;

	@Before
	public void setUp() {
		settableClock = new SettableClock();
		oneTimeTask = TestTasks.oneTime("OneTime", TestTasks.DO_NOTHING);
		nonCompletingExecutionHandler = new TestTasks.CountingHandler();
		deadExecutionHandler = new RescheduleDead();
		nonCompleting = new NonCompletingTask("NonCompleting", nonCompletingExecutionHandler, deadExecutionHandler);

		TaskResolver taskResolver = new TaskResolver(TaskResolver.OnCannotResolve.FAIL_ON_UNRESOLVED, oneTimeTask, nonCompleting);

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
	public void scheduler_should_handle_dead_executions() {
		final Instant now = settableClock.now();

		final TaskInstance taskInstance = oneTimeTask.instance("id1");
		final Execution execution1 = new Execution(now.minus(Duration.ofDays(1)), taskInstance);
		jdbcTaskRepository.createIfNotExists(execution1);

		final List<Execution<Object>> due = jdbcTaskRepository.getDue(now);
		assertThat(due, Matchers.hasSize(1));
		final Execution execution = due.get(0);
		final Optional<Execution<Object>> pickedExecution = jdbcTaskRepository.pick(execution, now);
		jdbcTaskRepository.updateHeartbeat(pickedExecution.get(), now.minus(Duration.ofHours(1)));

		scheduler.detectDeadExecutions();

		final Optional<Execution> rescheduled = jdbcTaskRepository.getExecution(taskInstance);
		assertTrue(rescheduled.isPresent());
		assertThat(rescheduled.get().picked, is(false));
		assertThat(rescheduled.get().pickedBy, nullValue());

		assertThat(jdbcTaskRepository.getDue(Instant.now()), hasSize(1));
	}

	@Test
	public void scheduler_should_detect_dead_execution_that_never_updated_heartbeat() {
		final Instant now = Instant.now();
		settableClock.set(now.minus(Duration.ofHours(1)));
		final Instant oneHourAgo = settableClock.now();

		final TaskInstance taskInstance = nonCompleting.instance("id1");
		final Execution execution1 = new Execution(oneHourAgo, taskInstance);
		jdbcTaskRepository.createIfNotExists(execution1);

		scheduler.executeDue();
		assertThat(nonCompletingExecutionHandler.timesExecuted, is(1));

		scheduler.executeDue();
		assertThat(nonCompletingExecutionHandler.timesExecuted, is(1));

		settableClock.set(Instant.now());

		scheduler.detectDeadExecutions();
		assertThat(deadExecutionHandler.timesCalled, is(1));

		settableClock.set(Instant.now());

		scheduler.executeDue();
		assertThat(nonCompletingExecutionHandler.timesExecuted, is(2));
	}

	public static class NonCompletingTask extends Task {
		private final ExecutionHandler handler;

		public NonCompletingTask(String name, ExecutionHandler handler, DeadExecutionHandler deadExecutionHandler) {
			super(name, new DoNothingCompletionHandler(), deadExecutionHandler);
			this.handler = handler;
		}

		@Override
		public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
			handler.execute(taskInstance, executionContext);
		}
	}

	public static class RescheduleDead extends DeadExecutionHandler.RescheduleDeadExecution {
		public int timesCalled = 0;

		@Override
		public void deadExecution(Execution execution, ExecutionOperations executionOperations) {
			timesCalled++;
			super.deadExecution(execution, executionOperations);
		}
	}

	public static class DoNothingCompletionHandler implements CompletionHandler {
		@Override
		public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
		}
	}
}
