package com.github.kagkarlsson.scheduler.task.helper;

import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.Priority;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.junit.jupiter.api.Test;

class OneTimeTaskTest {

  @Test
  public void should_have_default_priority() {
    OneTimeTask<Void> oneTimeTask = Tasks.oneTime("name").execute(TestTasks.DO_NOTHING);
    TaskInstance<Void> taskInstance = oneTimeTask.instance("id1");

    assertEquals(OneTimeTask.DEFAULT_PRIORITY, taskInstance.getPriority());
  }

  @Test
  public void should_override_default_priority() {
    OneTimeTask<Void> oneTimeTask =
        Tasks.oneTime("name").defaultPriority(Priority.LOW).execute(TestTasks.DO_NOTHING);
    TaskInstance<Void> taskInstance = oneTimeTask.instance("id1");

    assertEquals(Priority.LOW, taskInstance.getPriority());
  }
}
