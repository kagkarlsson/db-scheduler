package com.github.kagkarlsson.scheduler;

import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import co.unruly.matchers.OptionalMatchers;
import com.github.kagkarlsson.scheduler.TestTasks.SavingHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.VoidExecutionHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class SchedulerClientTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

    private ManualScheduler scheduler;
    private SettableClock settableClock;

    private TestTasks.CountingHandler<Void> onetimeTaskHandlerA;
    private OneTimeTask<Void> oneTimeTaskA;

    private TestTasks.CountingHandler<Void> onetimeTaskHandlerB;
    private OneTimeTask<Void> oneTimeTaskB;

    private ScheduleAnotherTaskHandler<Void> scheduleAnother;
    private OneTimeTask<Void> scheduleAnotherTask;

    private SavingHandler<String> savingHandler;
    private OneTimeTask<String> savingTask;

    @BeforeEach
    public void setUp() {
        settableClock = new SettableClock();

        onetimeTaskHandlerA = new TestTasks.CountingHandler<>();
        oneTimeTaskA = TestTasks.oneTime("OneTimeA", Void.class, onetimeTaskHandlerA);

        oneTimeTaskB = TestTasks.oneTime("OneTimeB", Void.class, onetimeTaskHandlerB);
        onetimeTaskHandlerB = new TestTasks.CountingHandler<>();

        scheduleAnother = new ScheduleAnotherTaskHandler<>(oneTimeTaskA.instance("secondTask"),
                settableClock.now().plusSeconds(1));
        scheduleAnotherTask = TestTasks.oneTime("ScheduleAnotherTask", Void.class, scheduleAnother);

        savingHandler = new SavingHandler<>();
        savingTask = TestTasks.oneTime("SavingTask", String.class, savingHandler);

        scheduler = TestHelper
                .createManualScheduler(DB.getDataSource(), oneTimeTaskA, oneTimeTaskB, scheduleAnotherTask, savingTask)
                .clock(settableClock).start();
    }

    @Test
    public void client_should_be_able_to_schedule_executions() {
        SchedulerClient client = SchedulerClient.Builder.create(DB.getDataSource()).build();
        client.schedule(oneTimeTaskA.instance("1"), settableClock.now());

        scheduler.runAnyDueExecutions();
        assertThat(onetimeTaskHandlerA.timesExecuted.get(), CoreMatchers.is(1));
    }

    @Test
    public void should_be_able_to_schedule_other_executions_from_an_executionhandler() {
        scheduler.schedule(scheduleAnotherTask.instance("1"), settableClock.now());
        scheduler.runAnyDueExecutions();
        assertThat(scheduleAnother.timesExecuted, CoreMatchers.is(1));
        assertThat(onetimeTaskHandlerA.timesExecuted.get(), CoreMatchers.is(0));

        scheduler.tick(ofSeconds(1));
        scheduler.runAnyDueExecutions();
        assertThat(onetimeTaskHandlerA.timesExecuted.get(), CoreMatchers.is(1));
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
        assertThat(client.getScheduledExecutions().size(), is(5));
        assertThat(countExecutionsForTask(client, oneTimeTaskA.getName(), Void.class), is(2));
        assertThat(countExecutionsForTask(client, oneTimeTaskB.getName(), Void.class), is(3));
        assertThat(client.getScheduledExecutionsForTask(oneTimeTaskB.getName(), Void.class).size(), is(3));
    }

    @Test
    public void client_should_be_able_to_fetch_single_scheduled_execution() {
        SchedulerClient client = SchedulerClient.Builder.create(DB.getDataSource(), oneTimeTaskA).build();
        client.schedule(oneTimeTaskA.instance("1"), settableClock.now());

        assertThat(client.getScheduledExecution(TaskInstanceId.of(oneTimeTaskA.getName(), "1")),
                not(OptionalMatchers.empty()));
        assertThat(client.getScheduledExecution(TaskInstanceId.of(oneTimeTaskA.getName(), "2")),
                OptionalMatchers.empty());
        assertThat(client.getScheduledExecution(TaskInstanceId.of(oneTimeTaskB.getName(), "1")),
                OptionalMatchers.empty());
    }

    @Test
    public void client_should_be_able_to_reschedule_executions() {
        String data1 = "data1";
        String data2 = "data2";

        scheduler.schedule(savingTask.instance("1", data1), settableClock.now().plusSeconds(1));
        scheduler.reschedule(savingTask.instance("1"), settableClock.now());
        scheduler.runAnyDueExecutions();
        assertThat(savingHandler.savedData, CoreMatchers.is(data1));

        scheduler.schedule(savingTask.instance("2", "none"), settableClock.now().plusSeconds(1));
        scheduler.reschedule(savingTask.instance("2"), settableClock.now(), data2);
        scheduler.runAnyDueExecutions();
        assertThat(savingHandler.savedData, CoreMatchers.is(data2));

        scheduler.tick(ofSeconds(1));
        scheduler.runAnyDueExecutions();
        assertThat(savingHandler.savedData, CoreMatchers.is(data2));
    }

    private int countAllExecutions(SchedulerClient client) {
        AtomicInteger counter = new AtomicInteger(0);
        client.fetchScheduledExecutions((ScheduledExecution<Object> execution) -> {
            counter.incrementAndGet();
        });
        return counter.get();
    }

    private <T> int countExecutionsForTask(SchedulerClient client, String taskName, Class<T> dataClass) {
        AtomicInteger counter = new AtomicInteger(0);
        client.fetchScheduledExecutionsForTask(taskName, dataClass, (ScheduledExecution<T> execution) -> {
            counter.incrementAndGet();
        });
        return counter.get();
    }

    public static class ScheduleAnotherTaskHandler<T> implements VoidExecutionHandler<T> {
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
