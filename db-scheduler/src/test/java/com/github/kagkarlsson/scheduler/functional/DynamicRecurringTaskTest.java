package com.github.kagkarlsson.scheduler.functional;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.PlainScheduleAndData;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTaskWithPersistentSchedule;
import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Daily;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static co.unruly.matchers.OptionalMatchers.contains;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicRecurringTaskTest {

    public static final ZoneId ZONE = ZoneId.systemDefault();
    private static final LocalDate DATE = LocalDate.of(2018, 3, 1);
    private static final LocalTime TIME = LocalTime.of(8, 0);
    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
        clock.set(ZonedDateTime.of(DATE, TIME, ZONE).toInstant());
    }

    @Test
    public void should_schedule_multiple_instances_with_different_schedules() {

        final String taskName = "dynamic-recurring";
        final RecurringTaskWithPersistentSchedule<PlainScheduleAndData> task =
            Tasks.recurringWithPersistentSchedule(taskName, PlainScheduleAndData.class)
            .execute((taskInstance, executionContext) -> {
                return CompletableFuture.completedFuture(null);
            });

        ManualScheduler scheduler = manualSchedulerFor(singletonList(task));
        scheduler.start();

        final PlainScheduleAndData schedule1 = new PlainScheduleAndData(new Daily(LocalTime.of(23, 51)));
        final PlainScheduleAndData schedule2 = new PlainScheduleAndData(new Daily(LocalTime.of(23, 50)));
        final PlainScheduleAndData schedule3 = new PlainScheduleAndData(new Daily(LocalTime.of(23, 55)));

        scheduler.schedule(task.schedulableInstance("id1", schedule1));
        scheduler.schedule(task.schedulableInstance("id2", schedule2));

        assertScheduled(scheduler, task.instanceId("id1"), LocalTime.of(23, 51), schedule1);
        assertScheduled(scheduler, task.instanceId("id2"), LocalTime.of(23, 50), schedule2);

        scheduler.reschedule(task.schedulableInstance("id1", schedule3));
        assertScheduled(scheduler, task.instanceId("id1"), LocalTime.of(23, 55), schedule3);
    }

    @Test
    public void should_support_statechanging_tasks() throws InterruptedException {
        final PersistentFixedDelaySchedule scheduleAndData1 = new PersistentFixedDelaySchedule(Schedules.fixedDelay(Duration.ofSeconds(10)), 1);

        final String taskName = "dynamic-recurring";
        final RecurringTaskWithPersistentSchedule<PersistentFixedDelaySchedule> task =
            Tasks.recurringWithPersistentSchedule(taskName, PersistentFixedDelaySchedule.class)
                .executeStateful((taskInstance, executionContext) -> {
                    return CompletableFuture.supplyAsync(() -> {
                        final PersistentFixedDelaySchedule persistentFixedDelaySchedule = taskInstance.getData().returnIncremented();
                        System.out.println(persistentFixedDelaySchedule);
                        return persistentFixedDelaySchedule;
                    });
                });

        ManualScheduler scheduler = manualSchedulerFor(singletonList(task));
        scheduler.start();


        scheduler.schedule(task.schedulableInstance("id1", scheduleAndData1));

        assertScheduled(scheduler, task.instanceId("id1"), clock.now(), scheduleAndData1); // FixedDelay has initial execution-time now()
        scheduler.runAnyDueExecutions();
        // Since execution is executed in an async way, we need to wait for a while to let the execution finish before asserting
        Thread.sleep(1000);
        assertScheduled(scheduler, task.instanceId("id1"), clock.now().plus(Duration.ofSeconds(10)), scheduleAndData1.returnIncremented());
    }

    private void assertScheduled(ManualScheduler scheduler, TaskInstanceId instanceId, LocalTime expectedExecutionTime, Object taskData) {
        assertScheduled(scheduler, instanceId, ZonedDateTime.of(DATE, expectedExecutionTime, ZONE).toInstant(), taskData);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private void assertScheduled(ManualScheduler scheduler, TaskInstanceId instanceId, Instant expectedExecutionTime, Object taskData) {
        Optional<ScheduledExecution<Object>> firstExecution = scheduler.getScheduledExecution(instanceId);
        assertThat(firstExecution.map(ScheduledExecution::getExecutionTime),
            contains(expectedExecutionTime));
        if (taskData != null) {
            assertEquals(taskData, firstExecution.get().getData());
        }
    }

    private ManualScheduler manualSchedulerFor(List<Task<?>> recurringTasks) {
        return TestHelper.createManualScheduler(postgres.getDataSource(), recurringTasks)
            .clock(clock)
            .build();
    }


    public static class PersistentFixedDelaySchedule extends PlainScheduleAndData {
        public PersistentFixedDelaySchedule(Schedule schedule, Integer data) {
            super(schedule, data);
        }

        public PersistentFixedDelaySchedule returnIncremented() {
            return new PersistentFixedDelaySchedule(super.getSchedule(), ((Integer)super.getData()) + 1);
        }
    }


}
