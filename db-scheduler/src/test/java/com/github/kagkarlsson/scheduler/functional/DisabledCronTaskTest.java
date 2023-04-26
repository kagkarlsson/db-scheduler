package com.github.kagkarlsson.scheduler.functional;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class DisabledCronTaskTest {

    public static final String RECURRING_A = "recurring-a";
    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
        clock.set(ZonedDateTime.of(LocalDate.of(2018, 3, 1), LocalTime.of(8, 0), ZoneId.systemDefault()).toInstant());
    }

    @Test
    public void should_remove_existing_executions_for_tasks_with_disabled_schedule() {
        RecurringTask<Void> recurringTask = Tasks.recurring(RECURRING_A, Schedules.cron("0 0 12 * * ?"))
                .execute(TestTasks.DO_NOTHING);

        ManualScheduler scheduler = manualSchedulerFor(recurringTask);
        scheduler.start();
        scheduler.stop();

        assertTrue(scheduler.getScheduledExecution(TaskInstanceId.of(RECURRING_A, RecurringTask.INSTANCE)).isPresent());

        RecurringTask<Void> disabledRecurringTask = Tasks.recurring(RECURRING_A, Schedules.cron("-"))
                .execute(TestTasks.DO_NOTHING);

        ManualScheduler restartedScheduler = manualSchedulerFor(disabledRecurringTask);
        restartedScheduler.start();
        restartedScheduler.stop();

        assertFalse(
                scheduler.getScheduledExecution(TaskInstanceId.of(RECURRING_A, RecurringTask.INSTANCE)).isPresent());

        restartedScheduler.start();

        assertFalse(
                scheduler.getScheduledExecution(TaskInstanceId.of(RECURRING_A, RecurringTask.INSTANCE)).isPresent());
    }

    private ManualScheduler manualSchedulerFor(RecurringTask<?> recurringTasks) {
        return TestHelper.createManualScheduler(postgres.getDataSource()).clock(clock)
                .startTasks(singletonList(recurringTasks)).build();
    }
}
