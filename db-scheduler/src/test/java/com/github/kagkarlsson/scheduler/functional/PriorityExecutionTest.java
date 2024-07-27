package com.github.kagkarlsson.scheduler.functional;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.RegisterExtension;

public class PriorityExecutionTest {

  private SettableClock clock;

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  @BeforeEach
  public void setUp() {
    clock = new SettableClock();
  }

  @RepeatedTest(10)
  public void test_immediate_execution() {
    String[] sequence = new String[] {"priority-3", "priority-2", "priority-1", "priority-0"};

    AtomicInteger index = new AtomicInteger();

    OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);

    TestableRegistry.Condition condition = TestableRegistry.Conditions.completed(4);
    TestableRegistry registry = TestableRegistry.create().waitConditions(condition).build();

    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), task)
            .threads(1) // 1 thread to force being sequential
            .pollingInterval(Duration.ofMinutes(1))
            .schedulerName(new SchedulerName.Fixed("test"))
            .statsRegistry(registry)
            .enablePrioritization()
            .build();

    stopScheduler.register(scheduler);

    // no matter when they are scheduled, the highest priority should always be executed first
    scheduler.schedule(
        task.instanceBuilder(sequence[3]).setPriority(-1).build(), clock.now().minus(3, MINUTES));

    scheduler.schedule(
        task.instanceBuilder(sequence[1]).setPriority(1).build(), clock.now().minus(2, MINUTES));

    scheduler.schedule(
        task.instanceBuilder(sequence[0]).setPriority(2).build(), clock.now().minus(1, MINUTES));

    scheduler.schedule(task.instanceBuilder(sequence[2]).setPriority(0).build(), clock.now());

    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          scheduler.start();
          condition.waitFor();

          List<ExecutionComplete> completed = registry.getCompleted();
          assertThat(completed, hasSize(4));

          completed.forEach(
              e -> {
                assertEquals(
                    sequence[index.getAndIncrement()], e.getExecution().taskInstance.getId());
              });

          registry.assertNoFailures();
        });
  }
}
