package com.github.kagkarlsson.scheduler.scrolling;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.ExecutionTimeAndId;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ScrollingPostgresTest {

  @RegisterExtension
  private static final EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  private SchedulerClient schedulerClient;
  private SchedulerClient prioritySchedulerClient;
  private OneTimeTask<Void> task;

  private DataSource getDataSource() {
    return DB.getDataSource();
  }

  private void performDatabaseSpecificSetup() {
    DbUtils.clearTables(getDataSource());
  }

  private final String taskName = "testTask";

  @BeforeEach
  void setUp() {
    performDatabaseSpecificSetup();

    task = TestTasks.oneTime(taskName, Void.class, TestTasks.DO_NOTHING);

    Scheduler scheduler =
        Scheduler.create(getDataSource(), task)
            .threads(1)
            .pollingInterval(java.time.Duration.ofSeconds(1))
            .build();
    schedulerClient = SchedulerClient.Builder.create(getDataSource(), task).build();

    Scheduler priorityScheduler =
        Scheduler.create(getDataSource(), task)
            .enablePriority()
            .threads(1)
            .pollingInterval(java.time.Duration.ofSeconds(1))
            .build();
    prioritySchedulerClient =
        SchedulerClient.Builder.create(getDataSource(), task).enablePriority().build();

    stopScheduler.register(scheduler, priorityScheduler);
  }

  @Test
  void test_scrolling() {
    createTestExecutions(10);

    var half = getScheduledForTask(f -> f.withPicked(false).limit(5));
    assertEquals(5, half.size());

    ExecutionTimeAndId middle = ExecutionTimeAndId.from(half.get(4));
    var secondPage = getScheduledForTask(f -> f.withPicked(false).after(middle).limit(10));
    assertEquals(5, secondPage.size());

    var thirdPage = getScheduledForTask(f -> f.withPicked(false).before(middle).limit(10));
    assertEquals(4, thirdPage.size());

    var allResults = getScheduledForTask(f -> f.withPicked(false).limit(200));
    assertEquals(10, allResults.size());

    var afterBoundary = ExecutionTimeAndId.from(allResults.get(0));
    var beforeBoundary = ExecutionTimeAndId.from(allResults.get(4));
    var rangeResults =
        getScheduledForTask(f -> f.withPicked(false).after(afterBoundary).before(beforeBoundary));
    assertEquals(3, rangeResults.size());
  }

  @Test
  void test_scrolling_with_priority() {
    // P0 = highest priority
    var now = Instant.now();
    prioritySchedulerClient.scheduleIfNotExists(
        task.instanceBuilder("P0").priority(99).build(), now.plusSeconds(10));
    prioritySchedulerClient.scheduleIfNotExists(
        task.instanceBuilder("P1").priority(98).build(), now.plusSeconds(10));
    prioritySchedulerClient.scheduleIfNotExists(
        task.instanceBuilder("P2").priority(97).build(), now.plusSeconds(10));
    prioritySchedulerClient.scheduleIfNotExists(
        task.instanceBuilder("P3").priority(96).build(), now.plusSeconds(10));
    prioritySchedulerClient.scheduleIfNotExists(
        task.instanceBuilder("P4").priority(95).build(), now.plusSeconds(10));
    prioritySchedulerClient.scheduleIfNotExists(
        task.instanceBuilder("P5").priority(94).build(), now.plusSeconds(10));

    var forwardResults =
        getScheduledForTask(prioritySchedulerClient, f -> f.withPicked(false).limit(2));
    assertEquals(2, forwardResults.size());
    assertEquals("P0", forwardResults.get(0).getTaskInstance().getId());
    assertEquals("P1", forwardResults.get(1).getTaskInstance().getId());

    ExecutionTimeAndId boundary = ExecutionTimeAndId.from(forwardResults.get(1)); // P1
    var backwardResults =
        getScheduledForTask(
            prioritySchedulerClient, f -> f.withPicked(false).before(boundary).limit(2));
    assertEquals(1, backwardResults.size());
    assertEquals("P0", backwardResults.get(0).getTaskInstance().getId());

    var allResults =
        getScheduledForTask(prioritySchedulerClient, f -> f.withPicked(false).limit(100));
    assertEquals(6, allResults.size());

    ExecutionTimeAndId afterBoundary = ExecutionTimeAndId.from(allResults.get(1)); // P1
    ExecutionTimeAndId beforeBoundary = ExecutionTimeAndId.from(allResults.get(4)); // P4
    var rangeResults =
        getScheduledForTask(
            prioritySchedulerClient,
            f -> f.withPicked(false).after(afterBoundary).before(beforeBoundary).limit(2));
    assertEquals(2, rangeResults.size());
    assertEquals("P2", rangeResults.get(0).getTaskInstance().getId());
    assertEquals("P3", rangeResults.get(1).getTaskInstance().getId());

    var beforeP5 = ExecutionTimeAndId.from(allResults.get(5)); // P5
    var beforeP5Page =
        getScheduledForTask(
            prioritySchedulerClient, f -> f.withPicked(false).before(beforeP5).limit(10));
    assertEquals(5, beforeP5Page.size());
    assertEquals("P4", beforeP5Page.get(0).getTaskInstance().getId());
    assertEquals("P3", beforeP5Page.get(1).getTaskInstance().getId());
    assertEquals("P2", beforeP5Page.get(2).getTaskInstance().getId());
    assertEquals("P1", beforeP5Page.get(3).getTaskInstance().getId());
    assertEquals("P0", beforeP5Page.get(4).getTaskInstance().getId());
  }

  private List<ScheduledExecution<Void>> getScheduledForTask(
      SchedulerClient client,
      Function<ScheduledExecutionsFilter, ScheduledExecutionsFilter> filterFn) {
    var filter = filterFn.apply(ScheduledExecutionsFilter.all());
    List<ScheduledExecution<Void>> results = new ArrayList<>();
    client.fetchScheduledExecutionsForTask(taskName, task.getDataClass(), filter, results::add);
    return results;
  }

  private List<ScheduledExecution<Void>> getScheduledForTask(
      Function<ScheduledExecutionsFilter, ScheduledExecutionsFilter> filterFn) {
    return getScheduledForTask(schedulerClient, filterFn);
  }

  private void createTestExecutions(int count) {
    for (int i = 1; i <= count; i++) {
      schedulerClient.scheduleIfNotExists(
          task.instanceBuilder("task-" + String.format("%03d", i)).build(),
          Instant.now().plusSeconds(i));
    }
  }
}
