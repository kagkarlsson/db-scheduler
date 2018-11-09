package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import com.github.kagkarlsson.scheduler.task.helper.ComposableTask.ExecutionHandlerWithExternalCompletion;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.google.common.collect.Lists;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SchedulerClientTest {

    @Rule
    public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();

    private ManualScheduler scheduler;
    private SettableClock settableClock;
    private OneTimeTask<Void> oneTimeTaskA;

    private TestTasks.CountingHandler<Void> onetimeTaskHandlerA;
    private TestTasks.CountingHandler<Void> onetimeTaskHandlerB;
    private ScheduleAnotherTaskHandler<Void> scheduleAnother;
    private OneTimeTask<Void> scheduleAnotherTask;
    private OneTimeTask<Void> oneTimeTaskB;

    @Before
    public void setUp() {
        settableClock = new SettableClock();
        onetimeTaskHandlerA = new TestTasks.CountingHandler<>();
        onetimeTaskHandlerB = new TestTasks.CountingHandler<>();
        oneTimeTaskA = TestTasks.oneTime("OneTimeA", Void.class, onetimeTaskHandlerA);
        oneTimeTaskB = TestTasks.oneTime("OneTimeB", Void.class, onetimeTaskHandlerB);

        scheduleAnother = new ScheduleAnotherTaskHandler<>(oneTimeTaskA.instance("secondTask"), settableClock.now().plusSeconds(1));
        scheduleAnotherTask = TestTasks.oneTime("ScheduleAnotherTask", Void.class, scheduleAnother);

        scheduler = TestHelper.createManualScheduler(DB.getDataSource(), oneTimeTaskA, oneTimeTaskB, scheduleAnotherTask).clock(settableClock).start();
    }

    @Test
    public void client_should_be_able_to_schedule_executions() {
        SchedulerClient client = SchedulerClient.Builder.create(DB.getDataSource()).build();
        client.schedule(oneTimeTaskA.instance("1"), settableClock.now());

        scheduler.runAnyDueExecutions();
        assertThat(onetimeTaskHandlerA.timesExecuted, CoreMatchers.is(1));
    }

    @Test
    public void should_be_able_to_schedule_other_executions_from_an_executionhandler() {
        scheduler.schedule(scheduleAnotherTask.instance("1"), settableClock.now());
        scheduler.runAnyDueExecutions();
        assertThat(scheduleAnother.timesExecuted, CoreMatchers.is(1));
        assertThat(onetimeTaskHandlerA.timesExecuted, CoreMatchers.is(0));

        scheduler.tick(ofSeconds(1));
        scheduler.runAnyDueExecutions();
        assertThat(onetimeTaskHandlerA.timesExecuted, CoreMatchers.is(1));
    }

    @Test
    public void client_should_be_able_to_fetch_executions_for_task() {
        SchedulerClient client = SchedulerClient.Builder.create(DB.getDataSource(), oneTimeTaskA, oneTimeTaskB).build();
        client.schedule(oneTimeTaskA.instance("1"), settableClock.now());
        client.schedule(oneTimeTaskA.instance("2"), settableClock.now());
        client.schedule(oneTimeTaskB.instance("10"), settableClock.now());
        client.schedule(oneTimeTaskB.instance("11"), settableClock.now());
        client.schedule(oneTimeTaskB.instance("12"), settableClock.now());

        assertThat(countAllExecutions(client), is(5));
        assertThat(countExecutionsForTask(client, oneTimeTaskA.getName(), Void.class), is(2));
        assertThat(countExecutionsForTask(client, oneTimeTaskB.getName(), Void.class), is(3));
    }

    private int countAllExecutions(SchedulerClient client) {
        AtomicInteger counter = new AtomicInteger(0);
        client.getScheduledExecutions((ScheduledExecution<Object> execution) -> {counter.incrementAndGet();});
        return counter.get();
    }

    private <T> int countExecutionsForTask(SchedulerClient client, String taskName, Class<T> dataClass) {
        AtomicInteger counter = new AtomicInteger(0);
        client.getScheduledExecutionsForTask(taskName, dataClass, (ScheduledExecution<T> execution) -> {counter.incrementAndGet();});
        return counter.get();
    }


    public static class ScheduleAnotherTaskHandler<T> implements ExecutionHandlerWithExternalCompletion<T> {
        public int timesExecuted = 0;
        private final TaskInstance<Void> secondTask;
        private final Instant instant;

        public ScheduleAnotherTaskHandler(TaskInstance<Void> secondTask, Instant instant) {
            this.secondTask = secondTask;
            this.instant = instant;
        }

        @Override
        public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
            executionContext.getSchedulerClient().schedule(secondTask, instant);
            this.timesExecuted++;
        }
    }
}
