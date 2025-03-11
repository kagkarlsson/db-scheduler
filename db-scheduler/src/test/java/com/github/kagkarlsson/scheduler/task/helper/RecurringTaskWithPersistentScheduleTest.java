package com.github.kagkarlsson.scheduler.task.helper;

import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.serializer.jackson.ScheduleAndDataForTest;
import com.github.kagkarlsson.scheduler.task.Priority;
import org.junit.jupiter.api.Test;

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
}
