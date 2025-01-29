package com.github.kagkarlsson.scheduler.task.helper;

import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.task.Priority;
import org.junit.jupiter.api.Test;

class CustomTaskTest {

  @Test
  public void should_have_default_priority() {
    CustomTask<Void> customTask =
        Tasks.custom("name", Void.class).execute((taskInstance, executionContext) -> null);

    assertEquals(CustomTask.DEFAULT_PRIORITY, customTask.getDefaultPriority());
  }

  @Test
  public void should_override_default_priority() {
    CustomTask<Void> customTask =
        Tasks.custom("name", Void.class)
            .defaultPriority(Priority.LOW)
            .execute((taskInstance, executionContext) -> null);

    assertEquals(Priority.LOW, customTask.getDefaultPriority());
  }
}
