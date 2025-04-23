package com.github.kagkarlsson.scheduler.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import co.unruly.matchers.TimeMatchers;
import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.helper.TestableListener;
import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ImmediateExecutionTest {

  private SettableClock clock;

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  @BeforeEach
  public void setUp() {
    clock = new SettableClock();
  }

  @Test
  public void test_immediate_execution() {
    Assertions.assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> {
          Instant now = Instant.now();
          OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);
          TestableListener.Condition completedCondition = TestableListener.Conditions.completed(1);
          TestableListener.Condition executeDueCondition =
              TestableListener.Conditions.ranExecuteDue(1);

          TestableListener listener =
              TestableListener.create()
                  .waitConditions(executeDueCondition, completedCondition)
                  .build();

          Scheduler scheduler = createAndStartScheduler(task, listener);
          executeDueCondition.waitFor();

          scheduler.schedule(task.instance("1"), clock.now());
          completedCondition.waitFor();

          List<ExecutionComplete> completed = listener.getCompleted();
          assertThat(completed, hasSize(1));
          completed.forEach(
              e -> {
                assertThat(e.getResult(), is(ExecutionComplete.Result.OK));
                Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
                assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
              });
          listener.assertNoFailures();
        });
  }

  @Test
  public void test_immediate_execution_by_completion_handler() {
    Assertions.assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> {
          Instant now = Instant.now();
          CustomTask<Integer> task =
              Tasks.custom("two-step", Integer.class)
                  .execute(
                      (taskInstance, executionContext) -> {
                        if (taskInstance.getData() >= 2) {
                          return new CompletionHandler.OnCompleteRemove<>();
                        } else {
                          return new CompletionHandler.OnCompleteReschedule<>(
                              TestSchedules.now(), taskInstance.getData() + 1);
                        }
                      });

          TestableListener.Condition completedCondition = TestableListener.Conditions.completed(2);
          TestableListener.Condition executeDueCondition =
              TestableListener.Conditions.ranExecuteDue(1);

          TestableListener listener =
              TestableListener.create()
                  .waitConditions(executeDueCondition, completedCondition)
                  .build();

          Scheduler scheduler = createAndStartScheduler(task, listener);
          executeDueCondition.waitFor();

          scheduler.schedule(task.instance("id1", 1), clock.now());
          completedCondition.waitFor();

          List<ExecutionComplete> completed = listener.getCompleted();
          assertThat(completed, hasSize(2));
          completed.forEach(
              e -> {
                assertThat(e.getResult(), is(ExecutionComplete.Result.OK));
                Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
                assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
              });
          assertEquals(scheduler.getScheduledExecutions().size(), 0);
          listener.assertNoFailures();
        });
  }

  private Scheduler createAndStartScheduler(Task task, TestableListener listener) {
    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), task)
            .pollingInterval(Duration.ofMinutes(1))
            .enableImmediateExecution()
            .schedulerName(new SchedulerName.Fixed("test"))
            .addSchedulerListener(listener)
            .build();
    stopScheduler.register(scheduler);

    scheduler.start();
    return scheduler;
  }
}
