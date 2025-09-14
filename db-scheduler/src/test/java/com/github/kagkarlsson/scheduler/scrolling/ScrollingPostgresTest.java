package com.github.kagkarlsson.scheduler.scrolling;

import static com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter.all;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.ExecutionTimeAndId;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ScrollingPostgresTest {

  @RegisterExtension
  public static final EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

  private static final Instant NOW = Instant.now();
  private static final TaskDescriptor<Void> A_TASK = TaskDescriptor.of("testTask");

  private DataSource getDataSource() {
    return DB.getDataSource();
  }

  @Test
  void test_scrolling() {
    var client = SchedulerClient.Builder.create(getDataSource()).build();

    createTestExecutions(client, 10);

    var firstHalf = getScheduledForTask(client, all().limit(5));
    assertEquals(5, firstHalf.size());

    ExecutionTimeAndId fifth = ExecutionTimeAndId.from(firstHalf.get(4));
    var secondHalf = getScheduledForTask(client, all().after(fifth).limit(10));
    assertEquals(5, secondHalf.size());

    var allBeforeFifth = getScheduledForTask(client, all().before(fifth).limit(10));
    assertEquals(4, allBeforeFifth.size());

    var allResults = getScheduledForTask(client, all().limit(200));
    assertEquals(10, allResults.size());

    var first = ExecutionTimeAndId.from(allResults.get(0));
    var betweenFirstAndFifth = getScheduledForTask(client, all().after(first).before(fifth));
    assertEquals(3, betweenFirstAndFifth.size());
  }

  @Test
  void test_scrolling_with_priority() {
    var client = SchedulerClient.Builder.create(getDataSource()).enablePriority().build();

    // P0 = highest priority
    Instant futureInstant = NOW.plusSeconds(10);
    client.scheduleIfNotExists(A_TASK.instance("P0").priority(99).build(), futureInstant);
    client.scheduleIfNotExists(A_TASK.instance("P1").priority(98).build(), futureInstant);
    client.scheduleIfNotExists(A_TASK.instance("P2").priority(97).build(), futureInstant);
    client.scheduleIfNotExists(A_TASK.instance("P3").priority(96).build(), futureInstant);
    client.scheduleIfNotExists(A_TASK.instance("P4").priority(95).build(), futureInstant);
    client.scheduleIfNotExists(A_TASK.instance("P5").priority(94).build(), futureInstant);

    var forwardResults = getScheduledForTask(client, all().limit(2));
    assertThat(idsFrom(forwardResults), contains("P0", "P1"));

    ExecutionTimeAndId p1 = ExecutionTimeAndId.from(forwardResults.get(1)); // P1
    var backwardResults = getScheduledForTask(client, all().before(p1).limit(2));
    assertThat(idsFrom(backwardResults), contains("P0"));

    var allResults = getScheduledForTask(client, all().limit(100));
    assertThat(idsFrom(allResults), contains("P0", "P1", "P2", "P3", "P4", "P5"));

    ExecutionTimeAndId p4 = ExecutionTimeAndId.from(allResults.get(4)); // P4

    var rangeResults = getScheduledForTask(client, all().after(p1).before(p4).limit(2));
    assertThat(idsFrom(rangeResults), contains("P2", "P3"));

    var p5 = ExecutionTimeAndId.from(allResults.get(5)); // P5
    var beforeP5 = getScheduledForTask(client, all().before(p5).limit(10));
    assertThat(idsFrom(beforeP5), contains("P4", "P3", "P2", "P1", "P0"));
  }

  private void createTestExecutions(SchedulerClient client, int count) {
    for (int i = 1; i <= count; i++) {
      client.scheduleIfNotExists(
          A_TASK.instance("task-" + String.format("%03d", i)).build(),
          Instant.now().plusSeconds(i));
    }
  }

  private List<ScheduledExecution<Void>> getScheduledForTask(
      SchedulerClient client, ScheduledExecutionsFilter filter) {
    return client.getScheduledExecutionsForTask(
        A_TASK.getTaskName(), A_TASK.getDataClass(), filter);
  }

  private static List<String> idsFrom(List<ScheduledExecution<Void>> firstPage) {
    return firstPage.stream().map(se -> se.getTaskInstance().getId()).toList();
  }
}
