package com.github.kagkarlsson.scheduler.task.helper;

import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.Priority;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

class RecurringTaskTest {

  public static final String RECURRING_A = "recurring-a";

  @Test
  public void should_have_default_priority() {
    RecurringTask<Void> recurringTask =
        Tasks.recurring(RECURRING_A, Schedules.daily(LocalTime.MIDNIGHT))
            .execute(TestTasks.DO_NOTHING);
    TaskInstance<Void> taskInstance = recurringTask.instance("id1");

    assertEquals(RecurringTask.DEFAULT_PRIORITY, taskInstance.getPriority());
  }

  @Test
  public void should_override_default_priority() {
    RecurringTask<Void> recurringTask =
        Tasks.recurring(RECURRING_A, Schedules.daily(LocalTime.MIDNIGHT))
            .defaultPriority(Priority.LOW)
            .execute(TestTasks.DO_NOTHING);
    TaskInstance<Void> taskInstance = recurringTask.instance("id1");

    assertEquals(Priority.LOW, taskInstance.getPriority());
  }
}
