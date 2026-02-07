package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.util.function.Supplier;

public class SchedulerTestHelper {
  private final Supplier<Scheduler> schedulerSupplierSupplier;

  public SchedulerTestHelper(Supplier<Scheduler> schedulerSupplier) {
    this.schedulerSupplierSupplier = schedulerSupplier;
  }

  public void assertThatExecution(TaskInstanceId instance) {}
}
