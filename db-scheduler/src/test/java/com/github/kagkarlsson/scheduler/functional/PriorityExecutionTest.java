package com.github.kagkarlsson.scheduler.functional;

import static java.time.Duration.ofMinutes;
import static java.time.Instant.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName.Fixed;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class PriorityExecutionTest {
  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  private final OneTimeTask<Void> oneTimeTask =
      TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);

  @Test
  public void test_when_prioritization_is_enabled() {
    TestableRegistry.Condition condition = TestableRegistry.Conditions.completed(4);
    TestableRegistry registry = TestableRegistry.create().waitConditions(condition).build();

    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), oneTimeTask)
            .threads(1) // 1 thread to force being sequential
            .pollingInterval(ofMinutes(1))
            .schedulerName(new Fixed("test"))
            .statsRegistry(registry)
            .enablePrioritization()
            .build();

    stopScheduler.register(scheduler);

    // no matter when they are scheduled, the highest priority should always be executed first
    scheduler.schedule(
        oneTimeTask.instanceBuilder("three").priority(3).build(),
        Instant.parse("2020-01-01T20:00:00Z"));

    scheduler.schedule(
        oneTimeTask.instanceBuilder("one").priority(1).build(),
        Instant.parse("2020-01-01T18:00:00Z"));

    scheduler.schedule(
        oneTimeTask.instanceBuilder("zero").priority(0).build(),
        Instant.parse("2020-01-01T10:00:00Z"));

    scheduler.schedule(
        oneTimeTask.instanceBuilder("two").priority(2).build(),
        Instant.parse("2020-01-01T09:00:00Z"));

    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          scheduler.start();
          condition.waitFor();

          List<ExecutionComplete> completed = registry.getCompleted();
          assertThat(completed, hasSize(4));

          // when prioritization is enabled
          // the order should be maintained according to the priorities of tasks
          List<Integer> orderOfPriorities =
              completed.stream()
                  .map(ExecutionComplete::getExecution)
                  .map(e -> e.taskInstance.getPriority())
                  .collect(Collectors.toList());

          assertThat(orderOfPriorities, contains(3, 2, 1, 0));

          // and executions times should come second in the order
          List<Instant> orderOfExecutionTimes =
              completed.stream()
                  .map(ExecutionComplete::getExecution)
                  .map(e -> e.executionTime)
                  .collect(Collectors.toList());
          assertThat(
              orderOfExecutionTimes,
              contains(
                  Instant.parse("2020-01-01T20:00:00Z"),
                  Instant.parse("2020-01-01T09:00:00Z"),
                  Instant.parse("2020-01-01T18:00:00Z"),
                  Instant.parse("2020-01-01T10:00:00Z")));

          registry.assertNoFailures();
        });
  }

  @Test
  public void test_when_prioritization_is_disabled() {
    TestableRegistry.Condition condition = TestableRegistry.Conditions.completed(4);
    TestableRegistry registry = TestableRegistry.create().waitConditions(condition).build();

    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), oneTimeTask)
            .threads(1) // 1 thread to force being sequential
            .pollingInterval(ofMinutes(1))
            .schedulerName(new Fixed("test"))
            .statsRegistry(registry)
            .build();

    stopScheduler.register(scheduler);

    // when prioritization is disabled priorities should be ignored
    scheduler.schedule(
        oneTimeTask.instanceBuilder("three").priority(3).build(),
        Instant.parse("2020-01-01T20:00:00Z"));

    scheduler.schedule(
        oneTimeTask.instanceBuilder("one").priority(1).build(),
        Instant.parse("2020-01-01T18:00:00Z"));

    scheduler.schedule(
        oneTimeTask.instanceBuilder("zero").priority(0).build(),
        Instant.parse("2020-01-01T10:00:00Z"));

    scheduler.schedule(
        oneTimeTask.instanceBuilder("two").priority(2).build(),
        Instant.parse("2020-01-01T09:00:00Z"));

    scheduler.schedule(oneTimeTask.instanceBuilder("two").priority(2).build(), now());

    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          scheduler.start();
          condition.waitFor();

          List<ExecutionComplete> completed = registry.getCompleted();
          assertThat(completed, hasSize(4));

          // when prioritization is disabled
          // the order should be maintained according to the execution times of tasks
          List<Instant> orderOfExecutionTimes =
              completed.stream()
                  .map(ExecutionComplete::getExecution)
                  .map(e -> e.executionTime)
                  .collect(Collectors.toList());
          assertThat(
              orderOfExecutionTimes,
              contains(
                  Instant.parse("2020-01-01T09:00:00Z"),
                  Instant.parse("2020-01-01T10:00:00Z"),
                  Instant.parse("2020-01-01T18:00:00Z"),
                  Instant.parse("2020-01-01T20:00:00Z")));

          // and priorities should be ignored
          List<Integer> orderOfPriorities =
              completed.stream()
                  .map(ExecutionComplete::getExecution)
                  .map(e -> e.taskInstance.getPriority())
                  .collect(Collectors.toList());

          assertThat(orderOfPriorities, contains(2, 0, 1, 3));

          registry.assertNoFailures();
        });
  }
}
