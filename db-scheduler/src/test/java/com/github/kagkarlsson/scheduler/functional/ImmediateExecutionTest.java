package com.github.kagkarlsson.scheduler.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import co.unruly.matchers.TimeMatchers;
import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ImmediateExecutionTest {

    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
    }

    @Test
    public void test_immediate_execution() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {

            Instant now = Instant.now();
            OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);
            TestableRegistry.Condition completedCondition = TestableRegistry.Conditions.completed(1);
            TestableRegistry.Condition executeDueCondition = TestableRegistry.Conditions.ranExecuteDue(1);

            TestableRegistry registry = TestableRegistry.create()
                    .waitConditions(executeDueCondition, completedCondition).build();

            Scheduler scheduler = createAndStartScheduler(task, registry);
            executeDueCondition.waitFor();

            scheduler.schedule(task.instance("1"), clock.now());
            completedCondition.waitFor();

            List<ExecutionComplete> completed = registry.getCompleted();
            assertThat(completed, hasSize(1));
            completed.forEach(e -> {
                assertThat(e.getResult(), is(ExecutionComplete.Result.OK));
                Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
                assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
            });
            registry.assertNoFailures();
        });
    }

    @Test
    public void test_immediate_execution_by_completion_handler() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {

            Instant now = Instant.now();
            CustomTask<Integer> task = Tasks.custom("two-step", Integer.class)
                    .execute((taskInstance, executionContext) -> {
                        if (taskInstance.getData() >= 2) {
                            return new CompletionHandler.OnCompleteRemove<>();
                        } else {
                            return new CompletionHandler.OnCompleteReschedule<>(TestSchedules.now(),
                                    taskInstance.getData() + 1);
                        }
                    });

            TestableRegistry.Condition completedCondition = TestableRegistry.Conditions.completed(2);
            TestableRegistry.Condition executeDueCondition = TestableRegistry.Conditions.ranExecuteDue(1);

            TestableRegistry registry = TestableRegistry.create()
                    .waitConditions(executeDueCondition, completedCondition).build();

            Scheduler scheduler = createAndStartScheduler(task, registry);
            executeDueCondition.waitFor();

            scheduler.schedule(task.instance("id1", 1), clock.now());
            completedCondition.waitFor();

            List<ExecutionComplete> completed = registry.getCompleted();
            assertThat(completed, hasSize(2));
            completed.forEach(e -> {
                assertThat(e.getResult(), is(ExecutionComplete.Result.OK));
                Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
                assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
            });
            assertEquals(scheduler.getScheduledExecutions().size(), 0);
            registry.assertNoFailures();
        });
    }

    private Scheduler createAndStartScheduler(Task task, TestableRegistry registry) {
        Scheduler scheduler = Scheduler.create(postgres.getDataSource(), task).pollingInterval(Duration.ofMinutes(1))
                .enableImmediateExecution().schedulerName(new SchedulerName.Fixed("test")).statsRegistry(registry)
                .build();
        stopScheduler.register(scheduler);

        scheduler.start();
        return scheduler;
    }

}
