package com.github.kagkarlsson.scheduler.task.helper;

import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.serializer.jackson.ScheduleAndDataForTest;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler;
import com.github.kagkarlsson.scheduler.task.Priority;
import com.github.kagkarlsson.scheduler.task.helper.Tasks.RecurringTaskWithPersistentScheduleBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RecurringTaskWithPersistentScheduleTest {

  @Test
  public void should_have_default_priority() {
    RecurringTaskWithPersistentSchedule<ScheduleAndDataForTest> recurringTask =
        Tasks.recurringWithPersistentSchedule("name", ScheduleAndDataForTest.class)
            .execute((taskInstance, executionContext) -> new ScheduleAndDataForTest(null, null));

    assertEquals(
        RecurringTaskWithPersistentSchedule.DEFAULT_PRIORITY, recurringTask.getDefaultPriority());
  }

  @Test
  public void should_override_default_priority() {
    RecurringTaskWithPersistentSchedule<ScheduleAndDataForTest> recurringTask =
        Tasks.recurringWithPersistentSchedule("name", ScheduleAndDataForTest.class)
            .defaultPriority(Priority.LOW)
            .execute((taskInstance, executionContext) -> new ScheduleAndDataForTest(null, null));

    assertEquals(Priority.LOW, recurringTask.getDefaultPriority());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void should_have_ReviveDeadExecution_as_default_DeadExecutionHandler(
      boolean statefulExecution) {
    var recurringTaskBuilder =
        Tasks.recurringWithPersistentSchedule("name", ScheduleAndDataForTest.class);
    RecurringTaskWithPersistentSchedule<ScheduleAndDataForTest> recurringTask =
        getRecurringTask(statefulExecution, recurringTaskBuilder);

    assertEquals(
        DeadExecutionHandler.ReviveDeadExecution.class,
        recurringTask.getDeadExecutionHandler().getClass());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void should_overwrite_DeadExecutionHandler(boolean statefulExecution) {
    DeadExecutionHandler<ScheduleAndDataForTest> deadExecutionHandler =
        new DeadExecutionHandler.CancelDeadExecution<>();

    var recurringTaskBuilder =
        Tasks.recurringWithPersistentSchedule("name", ScheduleAndDataForTest.class)
            .onDeadExecution(deadExecutionHandler);
    RecurringTaskWithPersistentSchedule<ScheduleAndDataForTest> recurringTask =
        getRecurringTask(statefulExecution, recurringTaskBuilder);

    assertEquals(deadExecutionHandler, recurringTask.getDeadExecutionHandler());
  }

  private static RecurringTaskWithPersistentSchedule<ScheduleAndDataForTest> getRecurringTask(
      boolean statefulExecution,
      RecurringTaskWithPersistentScheduleBuilder<ScheduleAndDataForTest> recurringTaskBuilder) {
    if (statefulExecution) {
      return recurringTaskBuilder.executeStateful(
          (taskInstance, executionContext) -> new ScheduleAndDataForTest(null, null));
    }
    return recurringTaskBuilder.execute(
        (taskInstance, executionContext) -> new ScheduleAndDataForTest(null, null));
  }
}
