package com.github.kagkarlsson.scheduler.scrolling;

import static com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter.all;
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
