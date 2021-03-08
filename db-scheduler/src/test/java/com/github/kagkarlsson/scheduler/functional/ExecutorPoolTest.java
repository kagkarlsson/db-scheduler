package com.github.kagkarlsson.scheduler.functional;

import co.unruly.matchers.TimeMatchers;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;


public class ExecutorPoolTest {
    private static final Logger DEBUG_LOG = LoggerFactory.getLogger(ExecutorPoolTest.class);
    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();
//    Enable if test gets flaky!
//    @RegisterExtension
//    public ChangeLogLevelsExtension changeLogLevels = new ChangeLogLevelsExtension(
//        new LogLevelOverride("com.github.kagkarlsson.scheduler.DueExecutionsBatch", Level.TRACE),
//        new LogLevelOverride("com.github.kagkarlsson.scheduler.Waiter", Level.DEBUG),
//        new LogLevelOverride("com.github.kagkarlsson.scheduler.Scheduler", Level.DEBUG)
//    );

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();

    }

    // FIXLATER: make into normal test when it is working properly
    @RepeatedTest(10)
    public void test_execute_until_none_left_happy() {
        DEBUG_LOG.info("Starting test_execute_until_none_left_happy");
        testExecuteUntilNoneLeft(2, 2, 20);
    }

    // FIXLATER: make into normal test when it is working properly
    @RepeatedTest(10)
    public void test_execute_until_none_left_low_polling_limit() {
        DEBUG_LOG.info("Starting test_execute_until_none_left_low_polling_limit");
        testExecuteUntilNoneLeft(2, 10, 20);
    }

    // FIXLATER: make into normal test when it is working properly
    @RepeatedTest(10)
    public void test_execute_until_none_left_high_volume() {
        DEBUG_LOG.info("Starting test_execute_until_none_left_high_volume");
        testExecuteUntilNoneLeft(12, 4, 200);
    }


    private void testExecuteUntilNoneLeft(int pollingLimit, int threads, int executionsToRun) {
        Instant now = Instant.now();
        OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);
        TestableRegistry.Condition condition = TestableRegistry.Conditions.completed(executionsToRun);
        TestableRegistry registry = TestableRegistry.create().waitConditions(condition).build();

        Scheduler scheduler = Scheduler.create(postgres.getDataSource(), task)
            .threads(threads)
            .pollingInterval(Duration.ofMinutes(1))
            .schedulerName(new SchedulerName.Fixed("test"))
            .statsRegistry(registry)
            .build();
        stopScheduler.register(scheduler);

        IntStream.range(0, executionsToRun).forEach(i -> scheduler.schedule(task.instance(String.valueOf(i)), clock.now()));

        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            scheduler.start();
            condition.waitFor();

            List<ExecutionComplete> completed = registry.getCompleted();
            assertThat(completed, hasSize(executionsToRun));
            completed.forEach(e -> {
                assertThat(e.getResult(), is(ExecutionComplete.Result.OK));
                Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
                assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
            });
            registry.assertNoFailures();
        }, waitingForConditionTimedOut(scheduler));
    }

    private String waitingForConditionTimedOut(Scheduler scheduler) {
        final String currentlyExecuting = scheduler.getCurrentlyExecuting().stream()
            .map(ce -> ce.getTaskInstance().getTaskAndInstance())
            .collect(Collectors.joining(","));

        List<String> scheduled = new ArrayList<>();
        scheduler.fetchScheduledExecutions(se -> scheduled.add(se.getTaskInstance().getTaskName() + "_" + se.getTaskInstance().getId()));

        return "Gave up waiting for condition. \n" +
            "Currently executing:\n" + currentlyExecuting +
            "scheduled:\n" + String.join(",", scheduled);
    }

}
