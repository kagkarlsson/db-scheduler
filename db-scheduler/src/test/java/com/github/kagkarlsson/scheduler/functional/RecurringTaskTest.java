package com.github.kagkarlsson.scheduler.functional;

import static co.unruly.matchers.OptionalMatchers.contains;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class RecurringTaskTest {

  public static final ZoneId ZONE = ZoneId.systemDefault();
  private static final LocalDate DATE = LocalDate.of(2018, 3, 1);
  private static final LocalTime TIME = LocalTime.of(8, 0);
  public static final String RECURRING_A = "recurring-a";
  private SettableClock clock;

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @BeforeEach
  public void setUp() {
    clock = new SettableClock();
    clock.set(ZonedDateTime.of(DATE, TIME, ZONE).toInstant());
  }

  @Test
  public void should_have_starttime_according_to_schedule_by_default() {

    RecurringTask<Void> recurringTask =
        Tasks.recurring(RECURRING_A, Schedules.daily(LocalTime.of(23, 59)))
            .execute(TestTasks.DO_NOTHING);

    ManualScheduler scheduler = manualSchedulerFor(singletonList(recurringTask));
    scheduler.start();

    assertScheduled(scheduler, RECURRING_A, LocalTime.of(23, 59));
  }

  @Test
  public void should_have_starttime_now_if_overridden_by_schedule() {

    RecurringTask<Void> recurringTask =
        Tasks.recurring(RECURRING_A, Schedules.fixedDelay(Duration.ofHours(1)))
            .execute(TestTasks.DO_NOTHING);

    ManualScheduler scheduler = manualSchedulerFor(singletonList(recurringTask));
    scheduler.start();

    assertScheduled(scheduler, RECURRING_A, TIME);
  }

  @Test
  public void
      should_update_preexisting_exeutions_with_new_schedule_if_new_next_execution_time_before_preexisting() {
    RecurringTask<Void> recurringTask =
        Tasks.recurring(RECURRING_A, Schedules.daily(LocalTime.of(23, 59)))
            .execute(TestTasks.DO_NOTHING);

    ManualScheduler scheduler = manualSchedulerFor(singletonList(recurringTask));

    scheduler.start();
    assertScheduled(scheduler, RECURRING_A, LocalTime.of(23, 59));

    // Add an additional execution-time to the daily schedule
    RecurringTask<Void> recurringTaskNewSchedule =
        Tasks.recurring(RECURRING_A, Schedules.daily(LocalTime.of(12, 0), LocalTime.of(23, 59)))
            .execute(TestTasks.DO_NOTHING);

    ManualScheduler schedulerUpdatedTask =
        manualSchedulerFor(singletonList(recurringTaskNewSchedule));

    // Simulate restart with updated schedule for task, execution now already exists
    schedulerUpdatedTask.start();

    assertScheduled(schedulerUpdatedTask, RECURRING_A, LocalTime.of(12, 0));
  }

  @Test
  public void
      should_update_preexisting_exeutions_with_new_deterministic_schedule_if_new_next_execution_time_after_preexisting() {
    RecurringTask<Void> recurringTask =
        Tasks.recurring(RECURRING_A, Schedules.daily(LocalTime.of(12, 0)))
            .execute(TestTasks.DO_NOTHING);

    ManualScheduler scheduler = manualSchedulerFor(singletonList(recurringTask));

    scheduler.start();
    assertScheduled(scheduler, RECURRING_A, LocalTime.of(12, 0));

    RecurringTask<Void> recurringTaskNewSchedule =
        Tasks.recurring(RECURRING_A, Schedules.daily(LocalTime.of(23, 59)))
            .execute(TestTasks.DO_NOTHING);

    ManualScheduler schedulerUpdatedTask =
        manualSchedulerFor(singletonList(recurringTaskNewSchedule));

    // Simulate restart with updated schedule for task, execution now already exists
    schedulerUpdatedTask.start();

    // Should have unchanged execution-time
    assertScheduled(schedulerUpdatedTask, RECURRING_A, LocalTime.of(23, 59));
  }

  @Test
  public void
      should_not_update_data_of_preexisting_exeutions_even_if_rescheduling_because_of_updated_schedule() {
    RecurringTask<Integer> recurringTask =
        Tasks.recurring(RECURRING_A, Schedules.daily(LocalTime.of(23, 59)), Integer.class)
            .initialData(1)
            .execute((taskInstance, executionContext) -> {});

    ManualScheduler scheduler = manualSchedulerFor(singletonList(recurringTask));

    scheduler.start();
    assertScheduled(scheduler, RECURRING_A, LocalTime.of(23, 59), 1);

    // Add an additional execution-time to the daily schedule
    RecurringTask<Integer> recurringTaskNewSchedule =
        Tasks.recurring(
                RECURRING_A,
                Schedules.daily(LocalTime.of(12, 0), LocalTime.of(23, 59)),
                Integer.class)
            .initialData(2)
            .execute((taskInstance, executionContext) -> {});

    ManualScheduler schedulerUpdatedTask =
        manualSchedulerFor(singletonList(recurringTaskNewSchedule));

    // Simulate restart with updated schedule for task, execution now already exists
    schedulerUpdatedTask.start();

    assertScheduled(schedulerUpdatedTask, RECURRING_A, LocalTime.of(12, 0), 1); // unchanged data
  }

  private void assertScheduled(
      ManualScheduler scheduler, String taskName, LocalTime expectedExecutionTime) {
    assertScheduled(scheduler, taskName, expectedExecutionTime, null);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private void assertScheduled(
      ManualScheduler scheduler,
      String taskName,
      LocalTime expectedExecutionTime,
      Object taskData) {
    Optional<ScheduledExecution<Object>> firstExecution =
        scheduler.getScheduledExecution(TaskInstanceId.of(taskName, RecurringTask.INSTANCE));
    assertThat(
        firstExecution.map(ScheduledExecution::getExecutionTime),
        contains(ZonedDateTime.of(DATE, expectedExecutionTime, ZONE).toInstant()));
    if (taskData != null) {
      assertEquals(taskData, firstExecution.get().getData());
    }
  }

  private ManualScheduler manualSchedulerFor(List<RecurringTask<?>> recurringTasks) {
    return TestHelper.createManualScheduler(postgres.getDataSource())
        .clock(clock)
        .startTasks(recurringTasks)
        .build();
  }
}
