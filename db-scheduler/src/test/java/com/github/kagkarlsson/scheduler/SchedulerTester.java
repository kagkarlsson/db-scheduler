package com.github.kagkarlsson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.task.State;
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

    public ExecutionAssertion isRemoved() {
      assertThat(scheduler.getScheduledExecution(instance)).isEmpty();
      assertThat(scheduler.getDeactivatedExecutions())
          .as("Execution %s/%s is removed", instance.getTaskName(), instance.getId())
          .noneMatch(e -> matches(e.taskInstance()));
      return this;
    }

    public ExecutionAssertion hasState(State expected) {
      var deactivated =
          scheduler.getDeactivatedExecutions().stream()
              .filter(e -> matches(e.taskInstance()))
              .findFirst();
      assertThat(deactivated)
          .as("Execution %s/%s has state %s", instance.getTaskName(), instance.getId(), expected)
          .hasValueSatisfying(e -> assertThat(e.state()).isEqualTo(expected));
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

    private boolean matches(TaskInstanceId other) {
      return instance.getTaskName().equals(other.getTaskName())
          && instance.getId().equals(other.getId());
    }
  }
}
