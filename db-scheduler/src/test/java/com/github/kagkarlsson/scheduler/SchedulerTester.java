package com.github.kagkarlsson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import java.time.Instant;

public class SchedulerTester {

  private final ManualScheduler scheduler;

  public SchedulerTester(ManualScheduler scheduler) {
    this.scheduler = scheduler;
  }

  public ExecutionAssertion assertThatExecution(TaskInstanceId instance) {
    return new ExecutionAssertion(instance);
  }

  public void assertNoExecution(TaskInstanceId instance) {
    assertThat(scheduler.getScheduledExecution(instance))
        .as("no execution exists for task-instance %s", instance)
        .isEmpty();
  }

  public class ExecutionAssertion {

    private final TaskInstanceId instance;

    ExecutionAssertion(TaskInstanceId instance) {
      this.instance = instance;
    }

    public ExecutionAssertion isScheduled() {
      assertThat(scheduler.getScheduledExecution(instance))
          .as("Execution %s/%s is scheduled", instance.getTaskName(), instance.getId())
          .isPresent();
      return this;
    }

    public ExecutionAssertion hasConsecutiveFailures(int expected) {
      assertThat(scheduler.getScheduledExecution(instance))
          .as(
              "Execution %s/%s has %d consecutive failures",
              instance.getTaskName(), instance.getId(), expected)
          .hasValueSatisfying(e -> assertThat(e.getConsecutiveFailures()).isEqualTo(expected));
      return this;
    }

    public ExecutionAssertion hasExecutionTime(Instant expected) {
      assertThat(scheduler.getScheduledExecution(instance))
          .as(
              "Execution %s/%s has execution time %s",
              instance.getTaskName(), instance.getId(), expected)
          .hasValueSatisfying(e -> assertThat(e.getExecutionTime()).isEqualTo(expected));
      return this;
    }
  }
}
