package com.github.kagkarlsson.scheduler.scrolling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.ScrollBoundary;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class ScrollBoundaryTest {

  @Test
  void testConstructorValidation() {
    Instant now = Instant.now();

    // Valid construction
    ScrollBoundary boundary = new ScrollBoundary(now, "task-123");
    assertEquals(now, boundary.getExecutionTime());
    assertEquals("task-123", boundary.getTaskInstanceId());

    // Null execution time should throw
    assertThrows(NullPointerException.class, () -> new ScrollBoundary(null, "task-123"));

    // Null task instance ID should throw
    assertThrows(NullPointerException.class, () -> new ScrollBoundary(now, null));
  }

  @Test
  void testFromScheduledExecution() {
    Instant executionTime = Instant.parse("2025-01-01T12:00:00Z");
    OneTimeTask<Void> task = TestTasks.oneTime("test-task", Void.class, (instance, context) -> {});
    Execution execution = new Execution(executionTime, task.instance("task-instance-123"));

    ScheduledExecution<Void> scheduledExecution = new ScheduledExecution<>(Void.class, execution);

    ScrollBoundary boundary = ScrollBoundary.from(scheduledExecution);

    assertEquals(executionTime, boundary.getExecutionTime());
    assertEquals("task-instance-123", boundary.getTaskInstanceId());
  }

  @Test
  void testEncodedScroll() {
    Instant executionTime = Instant.parse("2025-01-01T12:00:00Z");
    String taskInstanceId = "task-instance-123";

    ScrollBoundary original = new ScrollBoundary(executionTime, taskInstanceId);

    // Test encoding
    String scroll = original.toEncodedPointOfScroll();
    assertNotNull(scroll);
    assertFalse(scroll.isEmpty());

    // Test decoding
    ScrollBoundary decoded = ScrollBoundary.fromEncodedPoint(scroll);

    assertEquals(original.getExecutionTime(), decoded.getExecutionTime());
    assertEquals(original.getTaskInstanceId(), decoded.getTaskInstanceId());
    assertEquals(original, decoded);
  }

  @Test
  void testEncodedScrollWithSpecialCharacters() {
    Instant executionTime = Instant.parse("2025-01-01T12:00:00Z");
    String taskInstanceId = "task:with:colons-and-dashes_and_underscores";

    ScrollBoundary original = new ScrollBoundary(executionTime, taskInstanceId);

    String scroll = original.toEncodedPointOfScroll();
    ScrollBoundary decoded = ScrollBoundary.fromEncodedPoint(scroll);

    assertEquals(original, decoded);
  }

  @Test
  void testInvalidScrollDecoding() {
    // Invalid base64
    assertThrows(
        IllegalArgumentException.class, () -> ScrollBoundary.fromEncodedPoint("invalid-base64!@#"));

    // Valid base64 but invalid format (no colon)
    String invalidFormat = java.util.Base64.getEncoder().encodeToString("no-colon-here".getBytes());
    assertThrows(
        IllegalArgumentException.class, () -> ScrollBoundary.fromEncodedPoint(invalidFormat));

    // Valid base64 but invalid timestamp
    String invalidTimestamp =
        java.util.Base64.getEncoder().encodeToString("not-a-number:task-id".getBytes());
    assertThrows(
        IllegalArgumentException.class, () -> ScrollBoundary.fromEncodedPoint(invalidTimestamp));
  }

  @Test
  void testEqualsAndHashCode() {
    Instant executionTime = Instant.parse("2025-01-01T12:00:00Z");
    String taskInstanceId = "task-instance-123";

    ScrollBoundary boundary1 = new ScrollBoundary(executionTime, taskInstanceId);
    ScrollBoundary boundary2 = new ScrollBoundary(executionTime, taskInstanceId);
    ScrollBoundary boundary3 = new ScrollBoundary(executionTime.plusSeconds(1), taskInstanceId);
    ScrollBoundary boundary4 = new ScrollBoundary(executionTime, "different-task-id");

    // Equals
    assertEquals(boundary1, boundary2);
    assertNotEquals(boundary1, boundary3);
    assertNotEquals(boundary1, boundary4);
    assertNotEquals(null, boundary1);
    assertNotEquals("not-a-boundary", boundary1);

    // Hash code
    assertEquals(boundary1.hashCode(), boundary2.hashCode());
  }

  @Test
  void testToString() {
    Instant executionTime = Instant.parse("2025-01-01T12:00:00Z");
    String taskInstanceId = "task-instance-123";

    ScrollBoundary boundary = new ScrollBoundary(executionTime, taskInstanceId);
    String str = boundary.toString();

    assertTrue(str.contains("ScrollBoundary"));
    assertTrue(str.contains(executionTime.toString()));
    assertTrue(str.contains(taskInstanceId));
  }

  @Test
  void testScrollRoundTripWithVariousTimestamps() {
    // Test with various timestamp values (using millisecond precision since that's what's stored in
    // boundary)
    Instant[] testTimes = {
      Instant.EPOCH,
      Instant.parse("2025-01-01T00:00:00Z"),
      Instant.parse("2025-12-31T23:59:59Z"),
      Instant.ofEpochMilli(
          System.currentTimeMillis()) // Use millisecond precision to avoid precision loss
    };

    String taskInstanceId = "test-task-123";

    for (Instant time : testTimes) {
      ScrollBoundary original = new ScrollBoundary(time, taskInstanceId);
      String scroll = original.toEncodedPointOfScroll();
      ScrollBoundary decoded = ScrollBoundary.fromEncodedPoint(scroll);

      assertEquals(original, decoded, "Failed for time: " + time);
    }
  }
}
