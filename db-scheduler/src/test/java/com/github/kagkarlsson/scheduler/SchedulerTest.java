package com.github.kagkarlsson.scheduler;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.kagkarlsson.scheduler.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

public class SchedulerTest {

    private TestTasks.CountingHandler<Void> handler;
    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
        handler = new TestTasks.CountingHandler<>();
    }

    private Scheduler schedulerFor(Task<?>... tasks) {
        return schedulerFor(MoreExecutors.newDirectExecutorService(), tasks);
    }

    private Scheduler schedulerFor(ExecutorService executor, Task<?> ... tasks) {
        final StatsRegistry statsRegistry = StatsRegistry.NOOP;
        TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, Arrays.asList(tasks));
        JdbcTaskRepository taskRepository = new JdbcTaskRepository(postgres.getDataSource(), DEFAULT_TABLE_NAME, taskResolver, new SchedulerName.Fixed("scheduler1"));
        return new Scheduler(clock, taskRepository, taskResolver, 1, executor, new SchedulerName.Fixed("name"), new Waiter(Duration.ZERO), Duration.ofSeconds(1), false, statsRegistry, 10_000, Duration.ofDays(14), new ArrayList<>());
    }

    @Test
    public void scheduler_should_execute_task_when_exactly_due() {
        OneTimeTask<Void> oneTimeTask = TestTasks.oneTime("OneTime", Void.class, handler);
        Scheduler scheduler = schedulerFor(oneTimeTask);

        Instant executionTime = clock.now().plus(Duration.ofMinutes(1));
        scheduler.schedule(oneTimeTask.instance("1"), executionTime);

        scheduler.executeDue();
        assertThat(handler.timesExecuted, is(0));

        clock.set(executionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted, is(1));
    }

    @Test
    public void scheduler_should_execute_rescheduled_task_when_exactly_due() {
        OneTimeTask<Void> oneTimeTask = TestTasks.oneTime("OneTime", Void.class, handler);
        Scheduler scheduler = schedulerFor(oneTimeTask);

        Instant executionTime = clock.now().plus(Duration.ofMinutes(1));
        String instanceId = "1";
        TaskInstance<Void> oneTimeTaskInstance = oneTimeTask.instance(instanceId);
        scheduler.schedule(oneTimeTaskInstance, executionTime);
        Instant reScheduledExecutionTime = clock.now().plus(Duration.ofMinutes(2));
        scheduler.reschedule(oneTimeTaskInstance, reScheduledExecutionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted, is(0));

        clock.set(executionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted, is(0));

        clock.set(reScheduledExecutionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted, is(1));
    }

    @Test
    public void scheduler_should_not_execute_canceled_tasks() {
        OneTimeTask<Void> oneTimeTask = TestTasks.oneTime("OneTime", Void.class, handler);
        Scheduler scheduler = schedulerFor(oneTimeTask);

        Instant executionTime = clock.now().plus(Duration.ofMinutes(1));
        String instanceId = "1";
        TaskInstance<Void> oneTimeTaskInstance = oneTimeTask.instance(instanceId);
        scheduler.schedule(oneTimeTaskInstance, executionTime);
        scheduler.cancel(oneTimeTaskInstance);
        scheduler.executeDue();
        assertThat(handler.timesExecuted, is(0));

        clock.set(executionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted, is(0));
    }

    @Test
    public void scheduler_should_execute_recurring_task_and_reschedule() {
        RecurringTask<Void> recurringTask = TestTasks.recurring("Recurring", FixedDelay.of(Duration.ofHours(1)), handler);
        Scheduler scheduler = schedulerFor(recurringTask);

        scheduler.schedule(recurringTask.instance("single"), clock.now());
        scheduler.executeDue();

        assertThat(handler.timesExecuted, is(1));

        Instant nextExecutionTime = clock.now().plus(Duration.ofHours(1));
        clock.set(nextExecutionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted, is(2));
    }

    @Test
    public void scheduler_should_track_duration() throws InterruptedException {
        TestTasks.PausingHandler<Void> pausingHandler = new TestTasks.PausingHandler<>();
        OneTimeTask<Void> oneTimeTask = TestTasks.oneTime("OneTime", Void.class, pausingHandler);
        Scheduler scheduler = schedulerFor(Executors.newSingleThreadExecutor(), oneTimeTask);

        scheduler.schedule(oneTimeTask.instance("1"), clock.now());
        scheduler.executeDue();
        pausingHandler.waitForExecute.await();

        assertThat(scheduler.getCurrentlyExecuting(), hasSize(1));
        clock.set(clock.now.plus(Duration.ofMinutes(1)));

        assertThat(scheduler.getCurrentlyExecuting().get(0).getDuration(), is(Duration.ofMinutes(1)));

        pausingHandler.waitInExecuteUntil.countDown();
    }

    @Test
    public void should_expose_cause_of_failure_to_completion_handler() throws InterruptedException {
        TestTasks.ResultRegisteringFailureHandler<Void> failureHandler = new TestTasks.ResultRegisteringFailureHandler<>();
        Task<Void> oneTimeTask = ComposableTask.customTask("cause-testing-task", Void.class, TestTasks.REMOVE_ON_COMPLETE, failureHandler,
                (inst, ctx) -> { throw new RuntimeException("Failed!");});

        Scheduler scheduler = schedulerFor(oneTimeTask);

        scheduler.schedule(oneTimeTask.instance("1"), clock.now());
        scheduler.executeDue();
//        failureHandler.waitForNotify.await();

        assertThat(failureHandler.result, is(ExecutionComplete.Result.FAILED));
        assertThat(failureHandler.cause.get().getMessage(), is("Failed!"));

    }

}
