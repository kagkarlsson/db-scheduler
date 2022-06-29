package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.ComposableTask;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static java.time.Duration.ZERO;
import static java.time.Duration.between;
import static java.time.Duration.ofDays;
import static java.time.Duration.ofHours;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

public class SchedulerTest {

    private TestTasks.CountingHandler<Void> handler;
    private SettableClock clock;

    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();
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
        return schedulerFor(1, executor, tasks);
    }

    private Scheduler schedulerFor(int threadpoolSize, ExecutorService executor, Task<?> ... tasks) {
        final StatsRegistry statsRegistry = StatsRegistry.NOOP;
        TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, Arrays.asList(tasks));
        JdbcTaskRepository taskRepository = new JdbcTaskRepository(postgres.getDataSource(), false, DEFAULT_TABLE_NAME, taskResolver, new SchedulerName.Fixed("scheduler1"), clock);
        final Scheduler scheduler = new Scheduler(clock, taskRepository, taskRepository, taskResolver, threadpoolSize, executor,
            new SchedulerName.Fixed("name"), new Waiter(ZERO), ofSeconds(1), false,
            statsRegistry, PollingStrategyConfig.DEFAULT_FETCH, ofDays(14), ZERO, LogLevel.DEBUG, true, new ArrayList<>());
        stopScheduler.register(scheduler);
        return scheduler;
    }

    @Test
    public void scheduler_should_execute_task_when_exactly_due() {
        OneTimeTask<Void> oneTimeTask = TestTasks.oneTime("OneTime", Void.class, handler);
        Scheduler scheduler = schedulerFor(oneTimeTask);

        Instant executionTime = clock.now().plus(ofMinutes(1));
        scheduler.schedule(oneTimeTask.instance("1"), executionTime);

        scheduler.executeDue();
        assertThat(handler.timesExecuted.get(), is(0));

        clock.set(executionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted.get(), is(1));
    }

    @Test
    public void scheduler_should_execute_rescheduled_task_when_exactly_due() {
        OneTimeTask<Void> oneTimeTask = TestTasks.oneTime("OneTime", Void.class, handler);
        Scheduler scheduler = schedulerFor(oneTimeTask);

        Instant executionTime = clock.now().plus(ofMinutes(1));
        String instanceId = "1";
        TaskInstance<Void> oneTimeTaskInstance = oneTimeTask.instance(instanceId);
        scheduler.schedule(oneTimeTaskInstance, executionTime);
        Instant reScheduledExecutionTime = clock.now().plus(ofMinutes(2));
        scheduler.reschedule(oneTimeTaskInstance, reScheduledExecutionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted.get(), is(0));

        clock.set(executionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted.get(), is(0));

        clock.set(reScheduledExecutionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted.get(), is(1));
    }

    @Test
    public void scheduler_should_not_execute_canceled_tasks() {
        OneTimeTask<Void> oneTimeTask = TestTasks.oneTime("OneTime", Void.class, handler);
        Scheduler scheduler = schedulerFor(oneTimeTask);

        Instant executionTime = clock.now().plus(ofMinutes(1));
        String instanceId = "1";
        TaskInstance<Void> oneTimeTaskInstance = oneTimeTask.instance(instanceId);
        scheduler.schedule(oneTimeTaskInstance, executionTime);
        scheduler.cancel(oneTimeTaskInstance);
        scheduler.executeDue();
        assertThat(handler.timesExecuted.get(), is(0));

        clock.set(executionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted.get(), is(0));
    }

    @Test
    public void scheduler_should_execute_recurring_task_and_reschedule() {
        RecurringTask<Void> recurringTask = TestTasks.recurring("Recurring", FixedDelay.of(ofHours(1)), handler);
        Scheduler scheduler = schedulerFor(recurringTask);

        scheduler.schedule(recurringTask.instance("single"), clock.now());
        scheduler.executeDue();

        assertThat(handler.timesExecuted.get(), is(1));

        Instant nextExecutionTime = clock.now().plus(ofHours(1));
        clock.set(nextExecutionTime);
        scheduler.executeDue();
        assertThat(handler.timesExecuted.get(), is(2));
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
        clock.set(clock.now.plus(ofMinutes(1)));

        assertThat(scheduler.getCurrentlyExecuting().get(0).getDuration(), is(ofMinutes(1)));

        pausingHandler.waitInExecuteUntil.countDown();
    }

    @Test
    public void should_expose_cause_of_failure_to_completion_handler() throws InterruptedException {
        TestTasks.ResultRegisteringFailureHandler<Void> failureHandler = new TestTasks.ResultRegisteringFailureHandler<>();
        Task<Void> oneTimeTask = ComposableTask.customTask("cause-testing-task", Void.class, TestTasks.REMOVE_ON_COMPLETE, failureHandler,
                (inst, ctx) -> { return CompletableFuture.runAsync(() -> { throw new RuntimeException("Failed!"); });});

        Scheduler scheduler = schedulerFor(oneTimeTask);

        scheduler.schedule(oneTimeTask.instance("1"), clock.now());
        scheduler.executeDue();
        failureHandler.waitForNotify.await();

        assertThat(failureHandler.result, is(ExecutionComplete.Result.FAILED));
        assertThat(failureHandler.cause.get().getMessage(), is("java.lang.RuntimeException: Failed!"));

    }

    @Test
    public void should_only_attempt_task_when_max_retries_handler_used() throws InterruptedException {
        int maxRetries = RandomUtils.nextInt(1, 10);
        FailureHandler.MaxRetriesFailureHandler<Void> failureHandler = new FailureHandler.MaxRetriesFailureHandler<Void>(maxRetries, new FailureHandler.OnFailureRetryLater<>(ofMinutes(1)));

        OneTimeTask<Void> oneTimeTask = Tasks.oneTime("max-retries-task")
            .onFailure(failureHandler)
            .execute((inst, ctx) -> {
                return handler.execute(inst, ctx)
                    .thenRun(() -> { throw new RuntimeException("Failed!"); });
            });

        Scheduler scheduler = schedulerFor(oneTimeTask);

        scheduler.schedule(oneTimeTask.instance("1"), clock.now());
        scheduler.executeDue();

        //Simulate 15 minutes worth of time to validate we did not process more than we should
        for( int minuteWorthOfTime = 1; minuteWorthOfTime <= 15; minuteWorthOfTime ++) {
            clock.set(clock.now().plus(ofMinutes(1)));
            scheduler.executeDue();
        }

        //will always be maxRetries + 1 due to the first call always being required.
        assertThat(handler.timesExecuted.get(), is(maxRetries + 1));
    }

    @Test
    public void should_reschedule_failure_on_exponential_backoff_with_default_rate() throws InterruptedException {
        List<Instant> executionTimes = new ArrayList<>();

        Duration expectedSleepDuration = ofMinutes(1);
        OneTimeTask<Void> oneTimeTask = Tasks.oneTime("exponential-defaults-task")
            .onFailure(new FailureHandler.ExponentialBackoffFailureHandler<>(expectedSleepDuration))
            .execute((inst, ctx) -> {
                return CompletableFuture.runAsync(() -> {
                    executionTimes.add(ctx.getExecution().executionTime);
                    if(executionTimes.size() < 10){
                        throw new RuntimeException("Failed!");
                    }
                });
            });

        Scheduler scheduler = schedulerFor(oneTimeTask);

        Instant firstExecution = clock.now();
        scheduler.schedule(oneTimeTask.instance("1"), firstExecution);
        scheduler.executeDue();

        //Simulate 30 minutes worth of time to validate we did not process more than we should
        for( int minuteWorthOfTime = 1; minuteWorthOfTime <= 30; minuteWorthOfTime ++) {
            clock.set(clock.now().plus(ofMinutes(1)));
            scheduler.executeDue();
        }

        assertThat(executionTimes.size(), is(10));
        //Skip first execution of this b/c it was not using the exponential backoff but the first attempted call before failure
        for (int i = 1, executionTimesSize = executionTimes.size(); i < executionTimesSize; i++) {
            final Instant executionTime = executionTimes.get(i);
            long retryDurationMs = Math.round(expectedSleepDuration.toMillis() * Math.pow(1.5, i - 1));

            Duration scheduleTimeDifferenceFromFirstCall = between(firstExecution, executionTime);
            Duration actualExponentialBackoffDuration = ofMillis(retryDurationMs);
            assertThat(scheduleTimeDifferenceFromFirstCall.getSeconds(), greaterThanOrEqualTo(expectedSleepDuration.getSeconds()));
            assertThat(scheduleTimeDifferenceFromFirstCall.getSeconds(), is(actualExponentialBackoffDuration.getSeconds()));
        }
    }

    @Test
    public void should_reschedule_failure_on_exponential_backoff_with_defined_rate() throws InterruptedException {
        double customRate = 1.4;
        List<Instant> executionTimes = new ArrayList<>();

        Duration expectedSleepDuration = ofMinutes(1);
        OneTimeTask<Void> oneTimeTask = Tasks.oneTime("exponential-custom-rate-task")
            .onFailure(new FailureHandler.ExponentialBackoffFailureHandler<>(expectedSleepDuration, customRate))
            .execute((inst, ctx) -> {
                return CompletableFuture.runAsync(() -> {
                    executionTimes.add(ctx.getExecution().executionTime);
                    if(executionTimes.size() < 10){
                        throw new RuntimeException("Failed!");
                    }
                });
            });

        Scheduler scheduler = schedulerFor(oneTimeTask);

        Instant firstExecution = clock.now();
        scheduler.schedule(oneTimeTask.instance("1"), firstExecution);
        scheduler.executeDue();

        //Simulate 30 minutes worth of time to validate we did not process more than we should
        for( int minuteWorthOfTime = 1; minuteWorthOfTime <= 30; minuteWorthOfTime ++) {
            clock.set(clock.now().plus(ofMinutes(1)));
            scheduler.executeDue();
        }

        assertThat(executionTimes.size(), is(10));
        //Skip first execution of this b/c it was not using the exponential backoff but the first attempted call before failure
        for (int i = 1, executionTimesSize = executionTimes.size(); i < executionTimesSize; i++) {
            final Instant executionTime = executionTimes.get(i);
            long retryDurationMs = Math.round(expectedSleepDuration.toMillis() * Math.pow(customRate, i - 1));

            Duration scheduleTimeDifferenceFromFirstCall = between(firstExecution, executionTime);
            Duration actualExponentialBackoffDuration = ofMillis(retryDurationMs);
            assertThat(scheduleTimeDifferenceFromFirstCall.getSeconds(), greaterThanOrEqualTo(expectedSleepDuration.getSeconds()));
            assertThat(scheduleTimeDifferenceFromFirstCall.getSeconds(), is(actualExponentialBackoffDuration.getSeconds()));
        }
    }

}
