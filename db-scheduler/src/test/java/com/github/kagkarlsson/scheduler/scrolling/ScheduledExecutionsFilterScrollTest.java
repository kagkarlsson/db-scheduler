package com.github.kagkarlsson.scheduler.scrolling;

import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.ScrollBoundary;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class ScheduledExecutionsFilterScrollTest {

  @Test
  void testLimitValidation() {
    ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all();

    // Valid limit
    ScheduledExecutionsFilter result = filter.limit(100);
    assertEquals(100, result.getLimit().get());

    // Zero limit should throw
    assertThrows(IllegalArgumentException.class, () -> filter.limit(0));

    // Negative limit should throw
    assertThrows(IllegalArgumentException.class, () -> filter.limit(-1));
  }

  @Test
  void testAfterExecutionTime() {
    ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all();
    Instant time = Instant.parse("2025-01-01T12:00:00Z");

    ScheduledExecutionsFilter result = filter.afterExecutionTime(time);

    assertEquals(time, result.getAfterExecutionTime().get());
    assertFalse(result.getAfterTaskInstanceId().isPresent());
  }

  @Test
  void testAfterExecutionTimeWithTaskInstanceId() {
    ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all();
    Instant time = Instant.parse("2025-01-01T12:00:00Z");
    String taskInstanceId = "task-123";

    ScheduledExecutionsFilter result = filter.afterExecutionTime(time, taskInstanceId);

    assertEquals(time, result.getAfterExecutionTime().get());
    assertEquals(taskInstanceId, result.getAfterTaskInstanceId().get());
  }

  @Test
  void testBeforeExecutionTime() {
    ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all();
    Instant time = Instant.parse("2025-01-01T12:00:00Z");

    ScheduledExecutionsFilter result = filter.beforeExecutionTime(time);

    assertEquals(time, result.getBeforeExecutionTime().get());
    assertFalse(result.getBeforeTaskInstanceId().isPresent());
  }

  @Test
  void testBeforeExecutionTimeWithTaskInstanceId() {
    ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all();
    Instant time = Instant.parse("2025-01-01T12:00:00Z");
    String taskInstanceId = "task-123";

    ScheduledExecutionsFilter result = filter.beforeExecutionTime(time, taskInstanceId);

    assertEquals(time, result.getBeforeExecutionTime().get());
    assertEquals(taskInstanceId, result.getBeforeTaskInstanceId().get());
  }

  @Test
  void testAfterBoundary() {
    ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all();
    Instant time = Instant.parse("2025-01-01T12:00:00Z");
    String taskInstanceId = "task-123";
    ScrollBoundary boundary = new ScrollBoundary(time, taskInstanceId);

    ScheduledExecutionsFilter result = filter.after(boundary);

    assertEquals(time, result.getAfterExecutionTime().get());
    assertEquals(taskInstanceId, result.getAfterTaskInstanceId().get());
  }

  @Test
  void testBeforeBoundary() {
    ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all();
    Instant time = Instant.parse("2025-01-01T12:00:00Z");
    String taskInstanceId = "task-123";
    ScrollBoundary boundary = new ScrollBoundary(time, taskInstanceId);

    ScheduledExecutionsFilter result = filter.before(boundary);

    assertEquals(time, result.getBeforeExecutionTime().get());
    assertEquals(taskInstanceId, result.getBeforeTaskInstanceId().get());
  }

  @Test
  void testMethodChaining() {
    Instant afterTime = Instant.parse("2025-01-01T12:00:00Z");
    Instant beforeTime = Instant.parse("2025-01-01T18:00:00Z");

    ScheduledExecutionsFilter filter =
        ScheduledExecutionsFilter.all()
            .withPicked(false)
            .afterExecutionTime(afterTime, "after-task")
            .beforeExecutionTime(beforeTime, "before-task")
            .limit(50);

    // Check all values are set
    assertEquals(false, filter.getPickedValue().get());
    assertEquals(afterTime, filter.getAfterExecutionTime().get());
    assertEquals("after-task", filter.getAfterTaskInstanceId().get());
    assertEquals(beforeTime, filter.getBeforeExecutionTime().get());
    assertEquals("before-task", filter.getBeforeTaskInstanceId().get());
    assertEquals(50, filter.getLimit().get());
  }

  @Test
  void testAfterResetsTaskInstanceId() {
    ScheduledExecutionsFilter filter =
        ScheduledExecutionsFilter.all()
            .afterExecutionTime(Instant.parse("2025-01-01T12:00:00Z"), "initial-task");

    assertEquals("initial-task", filter.getAfterTaskInstanceId().get());

    // Call afterExecutionTime without taskInstanceId - should reset to null
    ScheduledExecutionsFilter updated =
        filter.afterExecutionTime(Instant.parse("2025-01-01T13:00:00Z"));

    assertFalse(updated.getAfterTaskInstanceId().isPresent());
  }

  @Test
  void testBeforeResetsTaskInstanceId() {
    ScheduledExecutionsFilter filter =
        ScheduledExecutionsFilter.all()
            .beforeExecutionTime(Instant.parse("2025-01-01T12:00:00Z"), "initial-task");

    assertEquals("initial-task", filter.getBeforeTaskInstanceId().get());

    // Call beforeExecutionTime without taskInstanceId - should reset to null
    ScheduledExecutionsFilter updated =
        filter.beforeExecutionTime(Instant.parse("2025-01-01T11:00:00Z"));

    assertFalse(updated.getBeforeTaskInstanceId().isPresent());
  }

  @Test
  void testEmptyOptionals() {
    ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all();

    assertFalse(filter.getAfterExecutionTime().isPresent());
    assertFalse(filter.getAfterTaskInstanceId().isPresent());
    assertFalse(filter.getBeforeExecutionTime().isPresent());
    assertFalse(filter.getBeforeTaskInstanceId().isPresent());
    assertFalse(filter.getLimit().isPresent());
  }

  @Test
  void testCombinedWithExistingFilters() {
    // Test that scrolling methods work with existing filter methods
    ScheduledExecutionsFilter filter =
        ScheduledExecutionsFilter.onlyResolved()
            .withPicked(true)
            .afterExecutionTime(Instant.parse("2025-01-01T12:00:00Z"))
            .limit(25);

    // Existing functionality should still work
    assertEquals(true, filter.getPickedValue().get());
    assertFalse(filter.getIncludeUnresolved());

    // New scrolling functionality should work
    assertTrue(filter.getAfterExecutionTime().isPresent());
    assertEquals(25, filter.getLimit().get());
  }
}
