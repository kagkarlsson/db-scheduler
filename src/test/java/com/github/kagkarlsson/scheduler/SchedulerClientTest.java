package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import com.github.kagkarlsson.scheduler.task.helper.ComposableTask.ExecutionHandlerWithExternalCompletion;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;

import static java.time.Duration.ofSeconds;
import static org.junit.Assert.assertThat;

public class SchedulerClientTest {

    @Rule
    public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();

    private ManualScheduler scheduler;
    private SettableClock settableClock;
    private OneTimeTask<Void> oneTimeTask;

    private TestTasks.CountingHandler<Void> onetimeTaskHandler;
    private ScheduleAnotherTaskHandler<Void> scheduleAnother;
    private OneTimeTask<Void> scheduleAnotherTask;

    @Before
    public void setUp() {
        settableClock = new SettableClock();
        onetimeTaskHandler = new TestTasks.CountingHandler<>();
        oneTimeTask = TestTasks.oneTime("OneTime", Void.class, onetimeTaskHandler);

        scheduleAnother = new ScheduleAnotherTaskHandler<>(oneTimeTask.instance("secondTask"), settableClock.now().plusSeconds(1));
        scheduleAnotherTask = TestTasks.oneTime("ScheduleAnotherTask", Void.class, scheduleAnother);

        scheduler = TestHelper.createManualScheduler(DB.getDataSource(), oneTimeTask, scheduleAnotherTask).clock(settableClock).start();
    }

    @Test
    public void client_should_be_able_to_schedule_executions() {
        SchedulerClient client = SchedulerClient.Builder.create(DB.getDataSource()).build();
        client.schedule(oneTimeTask.instance("1"), settableClock.now());

        scheduler.runAnyDueExecutions();
        assertThat(onetimeTaskHandler.timesExecuted, CoreMatchers.is(1));
    }

    @Test
    public void should_be_able_to_schedule_other_executions_from_an_executionhandler() {
        scheduler.schedule(scheduleAnotherTask.instance("1"), settableClock.now());
        scheduler.runAnyDueExecutions();
        assertThat(scheduleAnother.timesExecuted, CoreMatchers.is(1));
        assertThat(onetimeTaskHandler.timesExecuted, CoreMatchers.is(0));

        scheduler.tick(ofSeconds(1));
        scheduler.runAnyDueExecutions();
        assertThat(onetimeTaskHandler.timesExecuted, CoreMatchers.is(1));
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
