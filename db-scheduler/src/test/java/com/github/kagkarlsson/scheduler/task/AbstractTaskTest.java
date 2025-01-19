package com.github.kagkarlsson.scheduler.task;

import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Daily;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

class AbstractTaskTest {

  @Test
  void test_default_priority_with_data() {
    RecurringTask<String> task =
        Tasks.recurring("task", new Daily(LocalTime.MIDNIGHT), String.class)
            .executeStateful((taskInstance, executionContext) -> "");

    assertEquals(task.getDefaultPriority(), task.instance("id", "data").getPriority());
  }

  @Test
  void test_default_priority_without_data() {
    RecurringTask<Void> task =
        Tasks.recurring("task", new Daily(LocalTime.MIDNIGHT))
            .execute((taskInstance, executionContext) -> {});

    assertEquals(task.getDefaultPriority(), task.instance("id").getPriority());
  }

  @Test
  void test_default_priority_with_instance_builder() {
    RecurringTask<Void> task =
        Tasks.recurring("task", new Daily(LocalTime.MIDNIGHT))
            .execute((taskInstance, executionContext) -> {});

    assertEquals(task.getDefaultPriority(), task.instanceBuilder("id").build().getPriority());
  }
}
