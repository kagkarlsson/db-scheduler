package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.ComposableTask.ExecutionHandlerWithExternalCompletion;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.ExecutionHandler;
import com.github.kagkarlsson.scheduler.task.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.google.common.util.concurrent.MoreExecutors;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import static org.junit.Assert.assertThat;

public class SchedulerClientTest {

    @Rule
    public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();

    private Scheduler scheduler;
    private SettableClock settableClock;
    private OneTimeTask<Void> oneTimeTask;
    private JdbcTaskRepository jdbcTaskRepository;

    private TestTasks.CountingHandler<Void> onetimeTaskHandler;
    private ScheduleAnotherTaskHandler<Void> scheduleAnother;
    private OneTimeTask<Void> scheduleAnotherTask;

    @Before
    public void setUp() {
        settableClock = new SettableClock();
        onetimeTaskHandler = new TestTasks.CountingHandler<Void>();
        oneTimeTask = TestTasks.oneTime("OneTime", Void.class, onetimeTaskHandler);

        scheduleAnother = new ScheduleAnotherTaskHandler(oneTimeTask.instance("secondTask"), settableClock.now().plusSeconds(1));
        scheduleAnotherTask = TestTasks.oneTime("ScheduleAnotherTask", Void.class, scheduleAnother);

        TaskResolver taskResolver = new TaskResolver(oneTimeTask, scheduleAnotherTask);

        jdbcTaskRepository = new JdbcTaskRepository(DB.getDataSource(), taskResolver, new SchedulerName.Fixed("scheduler1"));

        scheduler = new Scheduler(settableClock,
                jdbcTaskRepository,
                taskResolver,
                1,
                MoreExecutors.newDirectExecutorService(),
                new SchedulerName.Fixed("test-scheduler"),
                new Waiter(Duration.ZERO),
                Duration.ofMinutes(1),
                StatsRegistry.NOOP,
                new ArrayList<>());

    }

    @Test
    public void client_should_be_able_to_schedule_executions() {
        SchedulerClient client = SchedulerClient.Builder.create(DB.getDataSource()).build();
        client.schedule(oneTimeTask.instance("1"), settableClock.now());

        scheduler.executeDue();
        assertThat(onetimeTaskHandler.timesExecuted, CoreMatchers.is(1));
    }

    @Test
    public void should_be_able_to_schedule_other_executions_from_an_executionhandler() {
        scheduler.schedule(scheduleAnotherTask.instance("1"), settableClock.now());
        scheduler.executeDue();
        assertThat(scheduleAnother.timesExecuted, CoreMatchers.is(1));
        assertThat(onetimeTaskHandler.timesExecuted, CoreMatchers.is(0));

        settableClock.set(settableClock.now().plusSeconds(1));
        scheduler.executeDue();
        assertThat(onetimeTaskHandler.timesExecuted, CoreMatchers.is(1));
    }

    public static class ScheduleAnotherTaskHandler<T> implements ExecutionHandlerWithExternalCompletion<T> {
        public int timesExecuted = 0;
        private TaskInstance<Void> secondTask;
        private Instant instant;

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
