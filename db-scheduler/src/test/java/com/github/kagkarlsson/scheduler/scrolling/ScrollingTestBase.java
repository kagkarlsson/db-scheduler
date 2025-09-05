package com.github.kagkarlsson.scheduler.scrolling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.ScrollBoundary;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Base class for scrolling integration tests. This class contains all the common test logic that
 * can be reused across different database engines. Concrete implementations should provide the
 * database-specific setup and DataSource.
 */
public abstract class ScrollingTestBase {

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  protected SchedulerClient schedulerClient;
  protected SchedulerClient prioritySchedulerClient;
  private OneTimeTask<Void> task;

  /**
   * Subclasses must provide a DataSource for the specific database being tested.
   *
   * @return the DataSource to use for testing
   */
  public abstract DataSource getDataSource();

  /**
   * Hook for database-specific setup that should run before each test. Default implementation
   * clears tables. Override if needed.
   */
  protected void performDatabaseSpecificSetup() {
    DbUtils.clearTables(getDataSource());
  }

  @BeforeEach
  final void setUp() {
    performDatabaseSpecificSetup();

    task = TestTasks.oneTime("test-task", Void.class, (instance, context) -> {});

    // Regular scheduler without priority
    Scheduler scheduler = Scheduler.create(getDataSource(), task).build();
    schedulerClient = scheduler;
    stopScheduler.register(scheduler);

    // Priority-enabled scheduler
    Scheduler priorityScheduler = Scheduler.create(getDataSource(), task).enablePriority().build();
    prioritySchedulerClient = priorityScheduler;
    stopScheduler.register(priorityScheduler);
  }

  private void createTestExecutions(int count) {
    // Create executions with execution times spread across different minutes
    for (int i = 1; i <= count; i++) {
      Instant executionTime = Instant.parse("2025-01-01T12:00:00Z").plusSeconds(i * 60L);
      String taskId = "task-" + String.format("%03d", i);
      schedulerClient.scheduleIfNotExists(task.instance(taskId), executionTime);
    }
  }

  private void createTestExecutionsWithPriority(int count, SchedulerClient client) {
    // Create executions with different priorities and execution times
    for (int i = 1; i <= count; i++) {
      Instant executionTime = Instant.parse("2025-01-01T12:00:00Z").plusSeconds(i * 60L);

      // Assign priorities: higher numbers first (10, 9, 8, ...)
      int priority = count - i + 1;

      client.scheduleIfNotExists(
          task.instanceBuilder("priority-task-" + String.format("%03d", i))
              .priority(priority)
              .build(),
          executionTime);
    }
  }

  @Test
  void testBasicScrollPagination() {
    createTestExecutions(10);

    List<ScheduledExecution<Object>> firstPage = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(3), firstPage::add);

    assertEquals(3, firstPage.size());

    // Get second page using scroll from last execution of first page
    ScrollBoundary boundary = ScrollBoundary.from(firstPage.get(firstPage.size() - 1));

    List<ScheduledExecution<Object>> secondPage = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(boundary).limit(3),
        secondPage::add);

    assertEquals(3, secondPage.size());

    // Verify no overlap between pages
    List<String> firstPageIds =
        firstPage.stream().map(e -> e.getTaskInstance().getId()).collect(Collectors.toList());
    List<String> secondPageIds = secondPage.stream().map(e -> e.getTaskInstance().getId()).toList();

    assertThat(firstPageIds, not(hasItems(secondPageIds.toArray(new String[0]))));

    // Verify ordering (execution time should be increasing)
    for (int i = 1; i < firstPage.size(); i++) {
      assertThat(
          firstPage.get(i).getExecutionTime(),
          greaterThan(firstPage.get(i - 1).getExecutionTime()));
    }

    // Second page executions should be after first page
    assertThat(
        secondPage.get(0).getExecutionTime(),
        greaterThan(firstPage.get(firstPage.size() - 1).getExecutionTime()));
  }

  @Test
  void testPriorityBasedScrollPagination() {
    createTestExecutionsWithPriority(6, prioritySchedulerClient);

    List<ScheduledExecution<Object>> firstPage = new ArrayList<>();
    prioritySchedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(3), firstPage::add);

    assertEquals(3, firstPage.size());

    // Verify priority ordering - should be highest priority first
    assertTrue(firstPage.get(0).getTaskInstance().getId().contains("priority-task-001")); // P=6
    assertTrue(firstPage.get(1).getTaskInstance().getId().contains("priority-task-002")); // P=5
    assertTrue(firstPage.get(2).getTaskInstance().getId().contains("priority-task-003")); // P=4

    // Get second page using scroll
    ScrollBoundary boundary = ScrollBoundary.from(firstPage.get(firstPage.size() - 1));

    List<ScheduledExecution<Object>> secondPage = new ArrayList<>();
    prioritySchedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(boundary).limit(3),
        secondPage::add);

    assertEquals(3, secondPage.size());

    // Verify no overlap
    List<String> firstPageIds =
        firstPage.stream().map(e -> e.getTaskInstance().getId()).collect(Collectors.toList());
    List<String> secondPageIds = secondPage.stream().map(e -> e.getTaskInstance().getId()).toList();

    assertThat(firstPageIds, not(hasItems(secondPageIds.toArray(new String[0]))));

    // Second page should continue priority ordering
    assertTrue(secondPage.get(0).getTaskInstance().getId().contains("priority-task-004")); // P=3
    assertTrue(secondPage.get(1).getTaskInstance().getId().contains("priority-task-005")); // P=2
    assertTrue(secondPage.get(2).getTaskInstance().getId().contains("priority-task-006")); // P=1
  }

  @Test
  void testLargeDatasetScrollPagination() {
    // Test with a larger dataset to verify pagination performance characteristics
    createTestExecutions(50);

    int pageSize = 5;
    int totalPages = 10;
    List<String> allIds = new ArrayList<>();

    ScrollBoundary scroll = null;

    for (int page = 0; page < totalPages; page++) {
      List<ScheduledExecution<Object>> currentPage = new ArrayList<>();

      ScheduledExecutionsFilter filter =
          ScheduledExecutionsFilter.all().withPicked(false).limit(pageSize);
      if (scroll != null) {
        filter = filter.after(scroll);
      }

      schedulerClient.fetchScheduledExecutions(filter, currentPage::add);

      assertEquals(
          pageSize, currentPage.size(), "Page " + page + " should have " + pageSize + " items");

      // Collect all IDs to verify no duplicates
      List<String> pageIds = currentPage.stream().map(e -> e.getTaskInstance().getId()).toList();
      allIds.addAll(pageIds);

      // Set scroll for next page
      scroll = ScrollBoundary.from(currentPage.get(currentPage.size() - 1));
    }

    // Verify all IDs are unique (no duplicates across pages)
    assertEquals(50, allIds.stream().distinct().count(), "Should have 50 unique executions");

    // Verify IDs are in order
    List<String> sortedIds = allIds.stream().sorted().collect(Collectors.toList());
    assertEquals(sortedIds, allIds, "IDs should be retrieved in sorted order");
  }

  @Test
  void testSingleItemScrollPagination() {
    createTestExecutions(5);

    // Test single item limit behavior
    List<ScheduledExecution<Object>> singleItem = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(1), singleItem::add);

    assertEquals(1, singleItem.size());

    // Use scroll to get next single item
    ScrollBoundary boundary = ScrollBoundary.from(singleItem.get(0));
    List<ScheduledExecution<Object>> nextSingle = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(boundary).limit(1),
        nextSingle::add);

    assertEquals(1, nextSingle.size());

    // Verify different executions
    assertNotEquals(
        singleItem.get(0).getTaskInstance().getId(), nextSingle.get(0).getTaskInstance().getId());

    // Verify correct ordering
    assertThat(
        nextSingle.get(0).getExecutionTime(), greaterThan(singleItem.get(0).getExecutionTime()));
  }

  @Test
  void testEncodedScroll() {
    createTestExecutions(6);

    // Test Base64 encoded scroll behavior
    List<ScheduledExecution<Object>> firstPage = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(3), firstPage::add);

    assertEquals(3, firstPage.size());

    // Create and encode scroll
    ScrollBoundary boundary = ScrollBoundary.from(firstPage.get(firstPage.size() - 1));
    String encodedScroll = boundary.toEncodedPointOfScroll();

    // Decode and use for next page
    ScrollBoundary decodedBoundary = ScrollBoundary.fromEncodedPoint(encodedScroll);

    List<ScheduledExecution<Object>> secondPage = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(decodedBoundary).limit(3),
        secondPage::add);

    assertEquals(3, secondPage.size());

    // Verify scroll worked correctly
    assertThat(
        secondPage.get(0).getExecutionTime(),
        greaterThan(firstPage.get(firstPage.size() - 1).getExecutionTime()));

    // Verify the decoded boundary matches the original
    assertEquals(boundary.getExecutionTime(), decodedBoundary.getExecutionTime());
    assertEquals(boundary.getTaskInstanceId(), decodedBoundary.getTaskInstanceId());
  }

  @Test
  void testScrollPaginationWithTaskSpecificFilter() {
    // Create mixed executions for different tasks
    OneTimeTask<Void> taskA = TestTasks.oneTime("task-a", Void.class, (instance, context) -> {});
    OneTimeTask<Void> taskB = TestTasks.oneTime("task-b", Void.class, (instance, context) -> {});

    // Create executions for both tasks interleaved by time
    for (int i = 1; i <= 10; i++) {
      Instant executionTime = Instant.parse("2025-01-01T12:00:00Z").plusSeconds(i * 60);
      if (i % 2 == 1) {
        schedulerClient.scheduleIfNotExists(taskA.instance("a-" + i), executionTime);
      } else {
        schedulerClient.scheduleIfNotExists(taskB.instance("b-" + i), executionTime);
      }
    }

    // Get first page of task-a executions only
    List<ScheduledExecution<Object>> firstPageTaskA = new ArrayList<>();
    schedulerClient.fetchScheduledExecutionsForTask(
        "task-a",
        Object.class,
        ScheduledExecutionsFilter.all().withPicked(false).limit(2),
        firstPageTaskA::add);

    assertEquals(2, firstPageTaskA.size());
    assertTrue(firstPageTaskA.get(0).getTaskInstance().getId().contains("a-"));
    assertTrue(firstPageTaskA.get(1).getTaskInstance().getId().contains("a-"));

    // Use scroll to get next page of task-a executions
    ScrollBoundary boundary = ScrollBoundary.from(firstPageTaskA.get(1));
    List<ScheduledExecution<Object>> secondPageTaskA = new ArrayList<>();
    schedulerClient.fetchScheduledExecutionsForTask(
        "task-a",
        Object.class,
        ScheduledExecutionsFilter.all().withPicked(false).after(boundary).limit(2),
        secondPageTaskA::add);

    assertEquals(2, secondPageTaskA.size());
    assertTrue(secondPageTaskA.get(0).getTaskInstance().getId().contains("a-"));
    assertTrue(secondPageTaskA.get(1).getTaskInstance().getId().contains("a-"));

    // Verify no overlap
    List<String> firstIds =
        firstPageTaskA.stream().map(e -> e.getTaskInstance().getId()).collect(Collectors.toList());
    List<String> secondIds =
        secondPageTaskA.stream().map(e -> e.getTaskInstance().getId()).toList();

    assertThat(firstIds, not(hasItems(secondIds.toArray(new String[0]))));
  }

  @Test
  void testTimestampPrecisionWithScroll() {
    // Test timestamp precision handling with close execution times
    Instant baseTime = Instant.parse("2025-01-01T12:00:00.123456Z");

    // Create executions with microsecond differences
    schedulerClient.scheduleIfNotExists(task.instance("microsec-1"), baseTime);
    schedulerClient.scheduleIfNotExists(
        task.instance("microsec-2"), baseTime.plusNanos(1000)); // +1µs
    schedulerClient.scheduleIfNotExists(
        task.instance("microsec-3"), baseTime.plusNanos(2000)); // +2µs
    schedulerClient.scheduleIfNotExists(
        task.instance("microsec-4"), baseTime.plusNanos(3000)); // +3µs

    List<ScheduledExecution<Object>> executions = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false), executions::add);

    assertEquals(4, executions.size());

    // Verify executions are ordered by execution time
    for (int i = 1; i < executions.size(); i++) {
      assertThat(
          "Execution " + i + " should be after execution " + (i - 1),
          executions.get(i).getExecutionTime(),
          greaterThanOrEqualTo(executions.get(i - 1).getExecutionTime()));
    }

    // Test scroll pagination with microsecond precision
    List<ScheduledExecution<Object>> firstTwo = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(2), firstTwo::add);

    ScrollBoundary boundary = ScrollBoundary.from(firstTwo.get(1));
    List<ScheduledExecution<Object>> remaining = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(boundary), remaining::add);

    assertEquals(2, firstTwo.size());
    assertEquals(2, remaining.size());

    // Verify no overlap
    List<String> firstIds =
        firstTwo.stream().map(e -> e.getTaskInstance().getId()).collect(Collectors.toList());
    List<String> remainingIds = remaining.stream().map(e -> e.getTaskInstance().getId()).toList();

    assertThat(firstIds, not(hasItems(remainingIds.toArray(new String[0]))));
  }

  @Test
  void testBeforeScrollPagination() {
    createTestExecutions(10);

    // Get all executions first
    List<ScheduledExecution<Object>> allExecutions = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false), allExecutions::add);

    assertEquals(10, allExecutions.size());

    // Use the 5th execution as a boundary to get executions before it
    ScrollBoundary boundary =
        ScrollBoundary.from(allExecutions.get(4)); // 0-indexed, so this is the 5th

    List<ScheduledExecution<Object>> beforeBoundary = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).before(boundary).limit(3),
        beforeBoundary::add);

    assertEquals(3, beforeBoundary.size());

    // Verify all returned executions are before the boundary
    for (ScheduledExecution<Object> execution : beforeBoundary) {
      assertThat(execution.getExecutionTime(), lessThan(boundary.getExecutionTime()));
    }

    // Verify these are the first 3 executions before the boundary (in ascending order)
    assertEquals("task-001", beforeBoundary.get(0).getTaskInstance().getId());
    assertEquals("task-002", beforeBoundary.get(1).getTaskInstance().getId());
    assertEquals("task-003", beforeBoundary.get(2).getTaskInstance().getId());
  }

  @Test
  void testScrollingLargeDatasetExhaustion() {
    // Test scrolling through 1000+ tasks to ensure scalability and proper end-of-dataset handling
    final int totalTasks = 1000;
    System.out.println("Creating " + totalTasks + " test executions for exhaustion test...");

    createTestExecutions(totalTasks);

    List<String> allCollectedIds = new ArrayList<>();
    ScrollBoundary scroll = null;
    int pageCount = 0;
    final int pageSize = 50; // Use reasonable page size for performance

    // Scroll through all tasks until exhausted
    while (true) {
      pageCount++;
      ScheduledExecutionsFilter filter =
          ScheduledExecutionsFilter.all().withPicked(false).limit(pageSize);
      if (scroll != null) {
        filter = filter.after(scroll);
      }

      List<ScheduledExecution<Object>> currentPage = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(filter, currentPage::add);

      if (currentPage.isEmpty()) {
        System.out.println(
            "Reached end of dataset at page "
                + pageCount
                + " with "
                + allCollectedIds.size()
                + " total records");
        break;
      }

      // Collect all IDs from this page
      currentPage.forEach(exec -> allCollectedIds.add(exec.getTaskInstance().getId()));

      // Set scroll for next page
      scroll = ScrollBoundary.from(currentPage.get(currentPage.size() - 1));

      // Safety check to prevent infinite loops in case of bugs
      if (pageCount > 25) { // 25 pages * 50 records = 1250 max, should be enough for 1000 records
        fail("Scrolling took too many pages (" + pageCount + "), possible infinite loop");
      }
    }

    // Verify we got exactly the expected number of unique records
    assertEquals(
        totalTasks, allCollectedIds.size(), "Should collect all " + totalTasks + " records");
    assertEquals(
        totalTasks,
        allCollectedIds.stream().distinct().count(),
        "All records should be unique - no duplicates");

    // Verify all expected task IDs are present
    for (int i = 1; i <= totalTasks; i++) {
      String expectedId = "task-" + String.format("%03d", i);
      assertTrue(allCollectedIds.contains(expectedId), "Missing expected task ID: " + expectedId);
    }

    System.out.println(
        "Successfully scrolled through "
            + totalTasks
            + " records in "
            + (pageCount - 1)
            + " pages");
  }

  @Test
  void testScrollingWithDataMutationCausesReappearance() {
    // Test what happens when execution_time changes between scrolling requests
    // This can cause executions to reappear in subsequent pages
    createTestExecutions(10);

    // Get first page
    List<ScheduledExecution<Object>> firstPage = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(3), firstPage::add);

    assertEquals(3, firstPage.size());
    System.out.println(
        "First page contains: "
            + firstPage.stream()
                .map(e -> e.getTaskInstance().getId())
                .collect(Collectors.joining(", ")));

    // Create boundary for second page
    ScrollBoundary boundary = ScrollBoundary.from(firstPage.get(firstPage.size() - 1));
    String lastTaskId = firstPage.get(firstPage.size() - 1).getTaskInstance().getId();

    // SIMULATE DATA MUTATION: Reschedule the last execution from first page to a future time
    // This should cause it to potentially reappear in subsequent scrolling
    Instant futureTime = Instant.parse("2025-01-01T18:00:00Z"); // Much later than original times
    schedulerClient.reschedule(firstPage.get(firstPage.size() - 1).getTaskInstance(), futureTime);

    System.out.println("Rescheduled " + lastTaskId + " to future time: " + futureTime);

    // Get second page using the boundary (which now refers to a moved execution)
    List<ScheduledExecution<Object>> secondPage = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(boundary).limit(5),
        secondPage::add);

    System.out.println(
        "Second page contains: "
            + secondPage.stream()
                .map(e -> e.getTaskInstance().getId())
                .collect(Collectors.joining(", ")));

    // Get all remaining executions to see the full picture
    List<ScheduledExecution<Object>> allRemainingExecutions = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false), allRemainingExecutions::add);

    System.out.println(
        "All executions after mutation: "
            + allRemainingExecutions.stream()
                .map(e -> e.getTaskInstance().getId() + "@" + e.getExecutionTime())
                .collect(Collectors.joining(", ")));

    // The rescheduled execution should now appear at the end (due to later execution time)
    ScheduledExecution<Object> rescheduledExecution =
        allRemainingExecutions.stream()
            .filter(e -> e.getTaskInstance().getId().equals(lastTaskId))
            .findFirst()
            .orElse(null);

    assertNotNull(rescheduledExecution, "Rescheduled execution should still exist");
    assertEquals(
        futureTime, rescheduledExecution.getExecutionTime(), "Execution should have new time");

    // Check if the rescheduled task appears in later pages (data reappearance)
    boolean rescheduledTaskInSecondPage =
        secondPage.stream().anyMatch(e -> e.getTaskInstance().getId().equals(lastTaskId));

    if (rescheduledTaskInSecondPage) {
      System.out.println(
          "⚠️  DATA REAPPEARANCE DETECTED: "
              + lastTaskId
              + " reappeared in second page after rescheduling");
      // This is the edge case - the execution reappears because its execution_time changed
      // This is expected behavior with scroll-based scrolling when data mutates
    } else {
      System.out.println("✅ No reappearance: " + lastTaskId + " did not reappear in second page");
    }

    // IMPORTANT: Test that we can still scroll through all data consistently
    // Even with data mutation, we should be able to get all current executions
    List<String> allCurrentIds = new ArrayList<>();
    ScrollBoundary currentScroll = null;

    while (true) {
      ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all().withPicked(false).limit(3);
      if (currentScroll != null) {
        filter = filter.after(currentScroll);
      }

      List<ScheduledExecution<Object>> page = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(filter, page::add);

      if (page.isEmpty()) break;

      page.forEach(exec -> allCurrentIds.add(exec.getTaskInstance().getId()));
      currentScroll = ScrollBoundary.from(page.get(page.size() - 1));
    }

    // Should still be able to get all 10 unique executions, even after mutation
    assertEquals(
        10,
        allCurrentIds.stream().distinct().count(),
        "Should still be able to scroll through all unique executions after data mutation");

    System.out.println(
        "✅ Successfully handled data mutation during scrolling - collected "
            + allCurrentIds.stream().distinct().count()
            + " unique executions");
  }

  @Test
  void testScrollingWithEmptyResults() {
    // Test scrolling with empty dataset and non-existent boundaries

    // No executions created - test empty dataset
    List<ScheduledExecution<Object>> results = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(5), results::add);

    assertEquals(0, results.size(), "Empty dataset should return no results");

    // Test scrolling after non-existent boundary
    ScrollBoundary fakeBoundary =
        new ScrollBoundary(Instant.parse("2025-01-01T12:00:00Z"), "non-existent-task");
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(fakeBoundary).limit(5),
        results::add);

    assertEquals(
        0, results.size(), "Non-existent boundary on empty dataset should return no results");

    // Create some executions and test boundary that doesn't exist in dataset
    createTestExecutions(5);

    List<ScheduledExecution<Object>> resultsAfterNonExistent = new ArrayList<>();
    ScrollBoundary nonExistentBoundary =
        new ScrollBoundary(Instant.parse("1999-01-01T00:00:00Z"), "never-existed");

    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(nonExistentBoundary).limit(3),
        resultsAfterNonExistent::add);

    assertEquals(
        3,
        resultsAfterNonExistent.size(),
        "Should return results since boundary is before all data");
    System.out.println("✅ Empty results and non-existent boundaries handled correctly");
  }

  @Test
  void testScrollingWithInvalidBoundaries() {
    // Test various invalid boundary scenarios
    createTestExecutions(5);

    // Test with completely invalid encoded scroll
    assertThrows(
        IllegalArgumentException.class,
        () -> ScrollBoundary.fromEncodedPoint("invalid-base64!@#$%^&*()"));

    // Test with valid base64 but invalid format (missing colon)
    String invalidFormat = Base64.getEncoder().encodeToString("invalid-format-no-colon".getBytes());
    assertThrows(
        IllegalArgumentException.class, () -> ScrollBoundary.fromEncodedPoint(invalidFormat));

    // Test with valid format but invalid timestamp
    String invalidTimestamp = Base64.getEncoder().encodeToString("not-a-number:task-id".getBytes());
    assertThrows(
        IllegalArgumentException.class, () -> ScrollBoundary.fromEncodedPoint(invalidTimestamp));

    // Test with empty scroll string
    assertThrows(IllegalArgumentException.class, () -> ScrollBoundary.fromEncodedPoint(""));

    // Test with null scroll string
    assertThrows(IllegalArgumentException.class, () -> ScrollBoundary.fromEncodedPoint(null));

    System.out.println("✅ Invalid boundary handling works correctly");
  }

  @Test
  void testScrollingCrossPageConsistencyNoDuplicatesOrGaps() {
    // Ensure no duplicates or missing records across all pages
    createTestExecutions(20);

    List<String> allCollectedIds = new ArrayList<>();
    ScrollBoundary scroll = null;
    int pageCount = 0;

    // Fetch all records in pages of 3
    while (true) {
      pageCount++;
      ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all().withPicked(false).limit(3);
      if (scroll != null) {
        filter = filter.after(scroll);
      }

      List<ScheduledExecution<Object>> page = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(filter, page::add);

      if (page.isEmpty()) break;

      // Check for duplicates within this page
      List<String> pageIds = page.stream().map(e -> e.getTaskInstance().getId()).toList();
      assertEquals(
          pageIds.size(),
          pageIds.stream().distinct().count(),
          "Page " + pageCount + " should not contain duplicates");

      // Check for duplicates across pages
      for (String pageId : pageIds) {
        assertFalse(
            allCollectedIds.contains(pageId),
            "Task " + pageId + " already seen in previous page - duplicate detected!");
        allCollectedIds.add(pageId);
      }

      scroll = ScrollBoundary.from(page.get(page.size() - 1));
    }

    // Verify we got exactly the expected number of unique records
    assertEquals(20, allCollectedIds.size(), "Should collect all 20 records");
    assertEquals(20, allCollectedIds.stream().distinct().count(), "All records should be unique");

    // Verify all expected task IDs are present (no gaps)
    for (int i = 1; i <= 20; i++) {
      String expectedId = "task-" + String.format("%03d", i);
      assertTrue(allCollectedIds.contains(expectedId), "Missing expected task ID: " + expectedId);
    }

    System.out.println(
        "✅ Cross-page consistency verified - no duplicates or gaps in " + pageCount + " pages");
  }

  @Test
  void testScrollingFromFirstAndLastRecordBoundaries() {
    // Test edge cases when scrolling from very first and last records
    createTestExecutions(10);

    // Get all executions to find first and last
    List<ScheduledExecution<Object>> allExecutions = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false), allExecutions::add);

    assertEquals(10, allExecutions.size());

    // Test scrolling from the very first record
    ScrollBoundary firstBoundary = ScrollBoundary.from(allExecutions.get(0));
    List<ScheduledExecution<Object>> afterFirst = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(firstBoundary).limit(5),
        afterFirst::add);

    assertEquals(5, afterFirst.size(), "Should get 5 records after first record");
    assertEquals(
        "task-002",
        afterFirst.get(0).getTaskInstance().getId(),
        "First result after first record should be task-002");

    // Test scrolling from the very last record
    ScrollBoundary lastBoundary = ScrollBoundary.from(allExecutions.get(allExecutions.size() - 1));
    List<ScheduledExecution<Object>> afterLast = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(lastBoundary).limit(5),
        afterLast::add);

    assertEquals(0, afterLast.size(), "Should get no records after the last record");

    // Test scrolling before the first record
    List<ScheduledExecution<Object>> beforeFirst = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).before(firstBoundary).limit(5),
        beforeFirst::add);

    assertEquals(0, beforeFirst.size(), "Should get no records before the first record");

    System.out.println("✅ First/last record boundary cases handled correctly");
  }

  @Test
  void testScrollingWithPickedStatusFilters() {
    // Test scrolling with picked/unpicked filter combinations
    createTestExecutions(10);

    // Get initial executions (all should be unpicked)
    List<ScheduledExecution<Object>> allInitial = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false), allInitial::add);

    assertEquals(10, allInitial.size());
    allInitial.forEach(
        exec -> assertFalse(exec.isPicked(), "Initial executions should not be picked"));

    // Test scrolling only unpicked executions
    List<ScheduledExecution<Object>> unpicked = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(5), unpicked::add);

    assertEquals(5, unpicked.size());
    unpicked.forEach(
        exec -> assertFalse(exec.isPicked(), "Filtered unpicked executions should not be picked"));

    // Test scrolling with boundary on unpicked executions
    ScrollBoundary boundary = ScrollBoundary.from(unpicked.get(unpicked.size() - 1));
    List<ScheduledExecution<Object>> nextUnpicked = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(boundary).limit(3),
        nextUnpicked::add);

    assertEquals(3, nextUnpicked.size());
    nextUnpicked.forEach(
        exec -> assertFalse(exec.isPicked(), "Next page of unpicked should not be picked"));

    // Verify no overlap between pages
    List<String> firstPageIds = unpicked.stream().map(e -> e.getTaskInstance().getId()).toList();
    List<String> secondPageIds =
        nextUnpicked.stream().map(e -> e.getTaskInstance().getId()).toList();

    firstPageIds.forEach(
        id -> assertFalse(secondPageIds.contains(id), "No task should appear in both pages"));

    System.out.println("✅ Picked status filter scrolling works correctly");
  }

  @Test
  void testScrollingWithExtremeLimitValues() {
    // Test edge cases with various limit values
    createTestExecutions(10);

    // Test with limit of 1 (minimum practical limit)
    List<ScheduledExecution<Object>> singleResult = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(1), singleResult::add);

    assertEquals(1, singleResult.size(), "Limit of 1 should return exactly 1 result");

    // Test scrolling with limit of 1 through multiple pages
    List<String> singlePageResults = new ArrayList<>();
    ScrollBoundary scroll = null;
    int pageCount = 0;

    while (pageCount < 5) { // Get first 5 records one by one
      pageCount++;
      ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all().withPicked(false).limit(1);
      if (scroll != null) {
        filter = filter.after(scroll);
      }

      List<ScheduledExecution<Object>> page = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(filter, page::add);

      if (page.isEmpty()) break;

      assertEquals(1, page.size(), "Each page should have exactly 1 result");
      singlePageResults.add(page.get(0).getTaskInstance().getId());
      scroll = ScrollBoundary.from(page.get(0));
    }

    assertEquals(5, singlePageResults.size(), "Should collect 5 individual results");
    assertEquals(
        5, singlePageResults.stream().distinct().count(), "All single results should be unique");

    // Test with very large limit (larger than dataset)
    List<ScheduledExecution<Object>> largeLimit = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(1000), largeLimit::add);

    assertEquals(10, largeLimit.size(), "Large limit should return all available records (10)");

    // Test invalid limits are rejected by filter validation
    assertThrows(
        IllegalArgumentException.class,
        () -> ScheduledExecutionsFilter.all().withPicked(false).limit(0),
        "Limit 0 should be rejected");

    assertThrows(
        IllegalArgumentException.class,
        () -> ScheduledExecutionsFilter.all().withPicked(false).limit(-1),
        "Negative limit should be rejected");

    System.out.println("✅ Extreme limit values handled correctly");
  }

  @Test
  void testScrollingWithSpecialCharacterTaskIds() {
    // Test scrolling with various special characters in task instance IDs
    String[] specialIds = {
      "task:with:colons",
      "task-with-dashes",
      "task_with_underscores",
      "task with spaces",
      "task@with@symbols",
      "task.with.dots",
      "task/with/slashes",
      "task'with'quotes",
      "task\"with\"doublequotes",
      "task(with)parentheses"
    };

    // Create executions with special character task IDs
    Instant baseTime = Instant.parse("2025-01-01T12:00:00Z");
    for (int i = 0; i < specialIds.length; i++) {
      schedulerClient.scheduleIfNotExists(
          task.instance(specialIds[i]), baseTime.plusSeconds(i * 60));
    }

    // Test basic scrolling works with special characters
    List<ScheduledExecution<Object>> results = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(4), results::add);

    assertEquals(4, results.size(), "Should retrieve 4 executions with special character IDs");

    // Test boundary encoding/decoding with special characters
    ScrollBoundary boundary = ScrollBoundary.from(results.get(results.size() - 1));
    String encodedBoundary = boundary.toEncodedPointOfScroll();

    assertNotNull(encodedBoundary, "Should be able to encode boundary with special character ID");
    assertFalse(encodedBoundary.isEmpty(), "Encoded boundary should not be empty");

    ScrollBoundary decodedBoundary = ScrollBoundary.fromEncodedPoint(encodedBoundary);
    assertEquals(boundary, decodedBoundary, "Decoded boundary should match original");

    // Test scrolling with the decoded boundary works
    List<ScheduledExecution<Object>> nextPage = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(decodedBoundary).limit(4),
        nextPage::add);

    assertThat(
        "Should get more results after boundary with special characters",
        nextPage.size(),
        greaterThan(0));

    // Verify we can scroll through all special character executions
    List<String> allCollected = new ArrayList<>();
    ScrollBoundary scroll = null;

    while (true) {
      ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all().withPicked(false).limit(3);
      if (scroll != null) {
        filter = filter.after(scroll);
      }

      List<ScheduledExecution<Object>> page = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(filter, page::add);

      if (page.isEmpty()) break;

      page.forEach(exec -> allCollected.add(exec.getTaskInstance().getId()));
      scroll = ScrollBoundary.from(page.get(page.size() - 1));
    }

    assertEquals(
        specialIds.length, allCollected.size(), "Should collect all special character executions");
    for (String specialId : specialIds) {
      assertTrue(
          allCollected.contains(specialId), "Should find special character ID: " + specialId);
    }

    System.out.println("✅ Special character task IDs handled correctly in scrolling");
  }

  @Test
  void testScrollingWithIdenticalExecutionTimes() {
    // Test tie-breaking when multiple executions have identical execution_time
    Instant sameTime = Instant.parse("2025-01-01T12:00:00Z");
    String[] taskIds = {"task-alpha", "task-beta", "task-gamma", "task-delta", "task-epsilon"};

    // Create multiple executions with identical execution times
    for (String taskId : taskIds) {
      schedulerClient.scheduleIfNotExists(task.instance(taskId), sameTime);
    }

    // Get all executions to verify they all have same execution time
    List<ScheduledExecution<Object>> allSameTime = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false), allSameTime::add);

    assertEquals(taskIds.length, allSameTime.size(), "Should have all executions with same time");
    allSameTime.forEach(
        exec ->
            assertEquals(
                sameTime, exec.getExecutionTime(), "All executions should have identical time"));

    // Test that scrolling works correctly with tie-breaking (using task_instance_id)
    List<ScheduledExecution<Object>> firstPage = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).limit(2), firstPage::add);

    assertEquals(2, firstPage.size(), "Should get 2 executions from first page");

    // Verify first page executions have same time but different task instance IDs
    assertEquals(firstPage.get(0).getExecutionTime(), firstPage.get(1).getExecutionTime());
    assertNotEquals(
        firstPage.get(0).getTaskInstance().getId(), firstPage.get(1).getTaskInstance().getId());

    // Test scrolling to second page using boundary from identical execution time
    ScrollBoundary boundary = ScrollBoundary.from(firstPage.get(firstPage.size() - 1));
    List<ScheduledExecution<Object>> secondPage = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).after(boundary).limit(2),
        secondPage::add);

    assertEquals(2, secondPage.size(), "Should get 2 more executions from second page");

    // Verify no duplicates between pages despite identical execution times
    List<String> firstPageIds = firstPage.stream().map(e -> e.getTaskInstance().getId()).toList();
    List<String> secondPageIds = secondPage.stream().map(e -> e.getTaskInstance().getId()).toList();

    firstPageIds.forEach(
        id ->
            assertFalse(
                secondPageIds.contains(id),
                "No duplicates should exist between pages with identical times"));

    // Collect all executions through scrolling and verify completeness
    List<String> allScrolledIds = new ArrayList<>();
    ScrollBoundary scroll = null;

    while (true) {
      ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all().withPicked(false).limit(2);
      if (scroll != null) {
        filter = filter.after(scroll);
      }

      List<ScheduledExecution<Object>> page = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(filter, page::add);

      if (page.isEmpty()) break;

      page.forEach(exec -> allScrolledIds.add(exec.getTaskInstance().getId()));
      scroll = ScrollBoundary.from(page.get(page.size() - 1));
    }

    assertEquals(
        taskIds.length,
        allScrolledIds.size(),
        "Should scroll through all executions with identical times");
    assertEquals(
        taskIds.length,
        allScrolledIds.stream().distinct().count(),
        "All scrolled executions should be unique");

    for (String expectedId : taskIds) {
      assertTrue(
          allScrolledIds.contains(expectedId), "Should find expected task ID: " + expectedId);
    }

    System.out.println("✅ Identical execution times with tie-breaking handled correctly");
  }

  @Test
  void testReverseScrollingComprehensiveEdgeCases() {
    // Comprehensive test for reverse scrolling using 'before' method
    createTestExecutions(15);

    // Get all executions to understand the complete dataset
    List<ScheduledExecution<Object>> allExecutions = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false), allExecutions::add);

    assertEquals(15, allExecutions.size(), "Should have 15 total executions");

    // TEST 1: Reverse scroll from the end (last execution)
    ScrollBoundary lastBoundary = ScrollBoundary.from(allExecutions.get(allExecutions.size() - 1));
    List<ScheduledExecution<Object>> beforeLast = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).before(lastBoundary).limit(3),
        beforeLast::add);

    assertEquals(3, beforeLast.size(), "Should get 3 executions before the last execution");

    // Verify the 'before' results are actually before the boundary
    Instant lastExecutionTime = allExecutions.get(allExecutions.size() - 1).getExecutionTime();
    beforeLast.forEach(
        exec ->
            assertTrue(
                exec.getExecutionTime().compareTo(lastExecutionTime) <= 0,
                "All 'before' results should have execution time <= boundary time"));

    System.out.println("✅ Reverse scrolling from end works correctly");

    // TEST 2: Reverse scroll from middle with multiple pages
    ScrollBoundary middleBoundary =
        ScrollBoundary.from(allExecutions.get(10)); // 11th execution (0-indexed)
    List<String> reverseCollectedIds = new ArrayList<>();
    ScrollBoundary currentBoundary = middleBoundary;
    int reversePageCount = 0;

    while (reversePageCount < 5) { // Limit to prevent infinite loops
      reversePageCount++;
      List<ScheduledExecution<Object>> reversePage = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(
          ScheduledExecutionsFilter.all().withPicked(false).before(currentBoundary).limit(2),
          reversePage::add);

      if (reversePage.isEmpty()) {
        System.out.println(
            "Reached beginning of dataset in reverse after " + reversePageCount + " pages");
        break;
      }

      // Collect IDs from this reverse page
      reversePage.forEach(exec -> reverseCollectedIds.add(exec.getTaskInstance().getId()));

      // Update boundary for next reverse page (use first execution of current page as next
      // boundary)
      currentBoundary = ScrollBoundary.from(reversePage.get(0));

      System.out.println(
          "Reverse page "
              + reversePageCount
              + ": "
              + reversePage.stream()
                  .map(e -> e.getTaskInstance().getId())
                  .collect(Collectors.joining(", ")));
    }

    assertFalse(reverseCollectedIds.isEmpty(), "Should collect some executions in reverse");
    assertEquals(
        reverseCollectedIds.size(),
        reverseCollectedIds.stream().distinct().count(),
        "All reverse collected executions should be unique");

    System.out.println(
        "✅ Multi-page reverse scrolling works correctly - collected "
            + reverseCollectedIds.size()
            + " unique executions");

    // TEST 3: Reverse scroll with identical execution times
    Instant sameTime = Instant.parse("2025-02-01T12:00:00Z");
    String[] identicalTimeIds = {"reverse-a", "reverse-b", "reverse-c", "reverse-d"};

    for (String taskId : identicalTimeIds) {
      schedulerClient.scheduleIfNotExists(task.instance(taskId), sameTime);
    }

    // Get all executions with the same time
    List<ScheduledExecution<Object>> sameTimeExecutions = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all()
            .withPicked(false)
            .afterExecutionTime(sameTime.minusSeconds(1))
            .beforeExecutionTime(sameTime.plusSeconds(1)),
        sameTimeExecutions::add);

    assertTrue(
        sameTimeExecutions.size() >= identicalTimeIds.length,
        "Should find executions with identical times");

    // Test reverse scrolling through identical execution times
    ScrollBoundary identicalTimeBoundary =
        ScrollBoundary.from(sameTimeExecutions.get(sameTimeExecutions.size() - 1));
    List<ScheduledExecution<Object>> beforeIdentical = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).before(identicalTimeBoundary).limit(3),
        beforeIdentical::add);

    assertFalse(
        beforeIdentical.isEmpty(), "Should get results even with identical execution times");
    System.out.println("✅ Reverse scrolling with identical execution times works correctly");

    // TEST 4: Reverse scroll from first execution (should return empty)
    ScrollBoundary firstBoundary = ScrollBoundary.from(allExecutions.get(0));
    List<ScheduledExecution<Object>> beforeFirst = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).before(firstBoundary).limit(5),
        beforeFirst::add);

    assertEquals(0, beforeFirst.size(), "Should get no results before the first execution");
    System.out.println("✅ Reverse scrolling from first execution correctly returns empty");

    // TEST 5: Reverse scroll with filters (picked status)
    List<ScheduledExecution<Object>> unpickedExecutions = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false), unpickedExecutions::add);

    if (unpickedExecutions.size() >= 5) {
      ScrollBoundary unpickedBoundary =
          ScrollBoundary.from(unpickedExecutions.get(unpickedExecutions.size() - 3));
      List<ScheduledExecution<Object>> beforeUnpicked = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(
          ScheduledExecutionsFilter.all().withPicked(false).before(unpickedBoundary).limit(4),
          beforeUnpicked::add);

      beforeUnpicked.forEach(
          exec ->
              assertFalse(
                  exec.isPicked(), "All reverse scrolled results should match the picked filter"));

      System.out.println("✅ Reverse scrolling with picked status filters works correctly");
    }

    // TEST 6: Reverse scroll with extreme boundary (far future)
    ScrollBoundary futureBoundary =
        new ScrollBoundary(Instant.parse("2030-01-01T00:00:00Z"), "future-task");
    List<ScheduledExecution<Object>> beforeFuture = new ArrayList<>();
    schedulerClient.fetchScheduledExecutions(
        ScheduledExecutionsFilter.all().withPicked(false).before(futureBoundary).limit(5),
        beforeFuture::add);

    assertFalse(beforeFuture.isEmpty(), "Should get results when boundary is in far future");
    System.out.println(
        "✅ Reverse scrolling with future boundary works correctly - got "
            + beforeFuture.size()
            + " results");

    // TEST 7: Bidirectional scrolling consistency
    // Pick a middle execution and scroll both directions, then verify complete coverage
    if (allExecutions.size() >= 10) {
      ScrollBoundary centerBoundary = ScrollBoundary.from(allExecutions.get(7)); // 8th execution

      // Forward from center
      List<String> forwardIds = new ArrayList<>();
      List<ScheduledExecution<Object>> forwardFromCenter = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(
          ScheduledExecutionsFilter.all().withPicked(false).after(centerBoundary).limit(4),
          forwardFromCenter::add);
      forwardFromCenter.forEach(exec -> forwardIds.add(exec.getTaskInstance().getId()));

      // Backward from center
      List<String> backwardIds = new ArrayList<>();
      List<ScheduledExecution<Object>> backwardFromCenter = new ArrayList<>();
      schedulerClient.fetchScheduledExecutions(
          ScheduledExecutionsFilter.all().withPicked(false).before(centerBoundary).limit(4),
          backwardFromCenter::add);
      backwardFromCenter.forEach(exec -> backwardIds.add(exec.getTaskInstance().getId()));

      // Verify no overlap between forward and backward
      forwardIds.forEach(
          forwardId ->
              assertFalse(
                  backwardIds.contains(forwardId),
                  "Forward and backward scrolling should not have overlapping results"));

      // Verify center execution is not in either set
      String centerId = allExecutions.get(7).getTaskInstance().getId();
      assertFalse(
          forwardIds.contains(centerId), "Center execution should not be in forward results");
      assertFalse(
          backwardIds.contains(centerId), "Center execution should not be in backward results");

      System.out.println(
          "✅ Bidirectional scrolling consistency verified - "
              + "forward: "
              + forwardIds.size()
              + ", backward: "
              + backwardIds.size()
              + " results");
    }

    System.out.println("✅ All reverse scrolling edge cases completed successfully!");
  }
}
