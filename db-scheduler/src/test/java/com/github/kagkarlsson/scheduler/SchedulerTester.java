package com.github.kagkarlsson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import java.time.Instant;
import java.util.List;

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

  public List<ScheduledExecution<Object>> getDeactivatedExecutions() {
    return scheduler.getScheduledExecutions(ScheduledExecutionsFilter.deactivated());
  }

  public class ExecutionAssertion {

    private final TaskInstanceId instance;

    ExecutionAssertion(TaskInstanceId instance) {
      this.instance = instance;
    }

    public ExecutionAssertion isScheduled() {
      return hasState(State.ACTIVE);
    }

    public ExecutionAssertion hasState(State expected) {
      var scheduled = scheduler.getScheduledExecution(instance);
      assertThat(scheduled)
          .as("Execution %s/%s has state %s", instance.getTaskName(), instance.getId(), expected)
          .hasValueSatisfying(e -> assertThat(e.getState()).isEqualTo(expected));
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

    public ExecutionAssertion hasNoLastFailure() {
      assertThat(scheduler.getScheduledExecution(instance))
          .as("Execution %s/%s has no last failure", instance.getTaskName(), instance.getId())
          .hasValueSatisfying(e -> assertThat(e.getLastFailure()).isNull());
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
