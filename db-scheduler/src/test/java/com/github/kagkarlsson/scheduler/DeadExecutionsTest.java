package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.VoidExecutionHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.google.common.util.concurrent.MoreExecutors;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.github.kagkarlsson.scheduler.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class DeadExecutionsTest {

    private static final int POLLING_LIMIT = 10_000;

    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

    private Scheduler scheduler;
    private SettableClock settableClock;
    private OneTimeTask<Void> oneTimeTask;
    private JdbcTaskRepository jdbcTaskRepository;
    private NonCompletingTask<Void> nonCompleting;
    private TestTasks.CountingHandler<Void> nonCompletingExecutionHandler;
    private ReviveDead<Void> deadExecutionHandler;

    @BeforeEach
    public void setUp() {
        settableClock = new SettableClock();
        oneTimeTask = TestTasks.oneTime("OneTime", Void.class, TestTasks.DO_NOTHING);
        nonCompletingExecutionHandler = new TestTasks.CountingHandler<>();
        deadExecutionHandler = new ReviveDead<>();
        nonCompleting = new NonCompletingTask<>("NonCompleting", Void.class, nonCompletingExecutionHandler, deadExecutionHandler);

        TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, oneTimeTask, nonCompleting);

        jdbcTaskRepository = new JdbcTaskRepository(DB.getDataSource(), false, DEFAULT_TABLE_NAME, taskResolver, new SchedulerName.Fixed("scheduler1"));

        scheduler = new Scheduler(settableClock,
            jdbcTaskRepository,
            jdbcTaskRepository,
            taskResolver,
            1,
            MoreExecutors.newDirectExecutorService(),
            new SchedulerName.Fixed("test-scheduler"),
            new Waiter(Duration.ZERO),
            Duration.ofMinutes(1),
            false,
            StatsRegistry.NOOP,
            PollingStrategyConfig.DEFAULT_FETCH,
            Duration.ofDays(14),
            Duration.ZERO,
            new ArrayList<>());

    }

    @Test
    public void scheduler_should_handle_dead_executions() {
        final Instant now = settableClock.now();

        final TaskInstance<Void> taskInstance = oneTimeTask.instance("id1");
        final Execution execution1 = new Execution(now.minus(Duration.ofDays(1)), taskInstance);
        jdbcTaskRepository.createIfNotExists(execution1);

        final List<Execution> due = jdbcTaskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, Matchers.hasSize(1));
        final Execution execution = due.get(0);
        final Optional<Execution> pickedExecution = jdbcTaskRepository.pick(execution, now);
        jdbcTaskRepository.updateHeartbeat(pickedExecution.get(), now.minus(Duration.ofHours(1)));

        scheduler.detectDeadExecutions();

        final Optional<Execution> rescheduled = jdbcTaskRepository.getExecution(taskInstance);
        assertTrue(rescheduled.isPresent());
        assertThat(rescheduled.get().picked, is(false));
        assertThat(rescheduled.get().pickedBy, nullValue());

        assertThat(jdbcTaskRepository.getDue(Instant.now(), POLLING_LIMIT), hasSize(1));
    }

    @Test
    public void scheduler_should_detect_dead_execution_that_never_updated_heartbeat() {
        final Instant now = Instant.now();
        settableClock.set(now.minus(Duration.ofHours(1)));
        final Instant oneHourAgo = settableClock.now();

        final TaskInstance<Void> taskInstance = nonCompleting.instance("id1");
        final Execution execution1 = new Execution(oneHourAgo, taskInstance);
        jdbcTaskRepository.createIfNotExists(execution1);

        scheduler.executeDue();
        assertThat(nonCompletingExecutionHandler.timesExecuted.get(), is(1));

        scheduler.executeDue();
        assertThat(nonCompletingExecutionHandler.timesExecuted.get(), is(1));

        settableClock.set(Instant.now());

        scheduler.detectDeadExecutions();
        assertThat(deadExecutionHandler.timesCalled, is(1));

        settableClock.set(Instant.now());

        scheduler.executeDue();
        assertThat(nonCompletingExecutionHandler.timesExecuted.get(), is(2));
    }

    public static class NonCompletingTask<T> extends Task<T> {
        private final VoidExecutionHandler<T> handler;

        public NonCompletingTask(String name, Class<T> dataClass, VoidExecutionHandler<T> handler, DeadExecutionHandler<T> deadExecutionHandler) {
            super(name, dataClass, (executionComplete, executionOperations) -> {}, deadExecutionHandler);
            this.handler = handler;
        }

        @Override
        public CompletionHandler<T> execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
            handler.execute(taskInstance, executionContext);
            throw new RuntimeException("simulated unexpected exception");
        }
    }

    public static class ReviveDead<T> extends DeadExecutionHandler.ReviveDeadExecution<T> {
        public int timesCalled = 0;

        @Override
        public void deadExecution(Execution execution, ExecutionOperations<T> executionOperations) {
            timesCalled++;
            super.deadExecution(execution, executionOperations);
        }
    }

}
