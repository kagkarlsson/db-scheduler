package com.github.kagkarlsson.scheduler.functional;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.RegisterExtension;


public class PriorityExecutionTest {

    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
    }

    @RepeatedTest(10)
    public void test_immediate_execution() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(100), () -> {

            String[] sequence = new String[]{"priority-3", "priority-2", "priority-1", "priority-0"};

            AtomicInteger index = new AtomicInteger();
            OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, (taskInstance, executionContext) -> {
                // check that the ordering is always correct
                Assertions.assertEquals(sequence[index.getAndIncrement()], taskInstance.getId());
            });
            TestableRegistry.Condition completedCondition = TestableRegistry.Conditions.completed(1);
            TestableRegistry.Condition executeDueCondition = TestableRegistry.Conditions.ranExecuteDue(1);

            TestableRegistry registry =
                TestableRegistry.create().waitConditions(executeDueCondition, completedCondition).build();

            Scheduler scheduler = Scheduler.create(postgres.getDataSource(), task)
                .pollingInterval(Duration.ofMinutes(1))
                .enableImmediateExecution()
                .schedulerName(new SchedulerName.Fixed("test"))
                .statsRegistry(registry)
                // 1 thread to force being sequential
                .threads(1)
                .build();
            stopScheduler.register(scheduler);

            // no matter when they are scheduled, the highest priority should always be executed first
            scheduler.schedule(task.instanceBuilder(sequence[3]).setPriority(-1).build(),
                clock.now().minus(3, ChronoUnit.MINUTES));
            scheduler.schedule(task.instanceBuilder(sequence[1]).setPriority(1).build(),
                clock.now().minus(2, ChronoUnit.MINUTES));
            scheduler.schedule(task.instanceBuilder(sequence[0]).setPriority(2).build(),
                clock.now().minus(1, ChronoUnit.MINUTES));
            scheduler.schedule(task.instanceBuilder(sequence[2]).setPriority(0).build(), clock.now());

            scheduler.start();
            executeDueCondition.waitFor();
            completedCondition.waitFor();
        });
    }
}
