package com.github.kagkarlsson.scheduler.scrolling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.kagkarlsson.scheduler.ExecutionTimeAndId;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ExecutionTimeAndIdTest {

  @Test
  void testConstruction() {
    var time = Instant.now();
    String taskId = "task-001";

    ExecutionTimeAndId boundary = new ExecutionTimeAndId(time, taskId);

    assertEquals(time, boundary.executionTime());
    assertEquals(taskId, boundary.taskInstanceId());
  }

  @Test
  void testNullValidation() {
    var time = Instant.now();
    String taskId = "task-001";

    assertThrows(NullPointerException.class, () -> new ExecutionTimeAndId(null, taskId));
    assertThrows(NullPointerException.class, () -> new ExecutionTimeAndId(time, null));
  }

  @Test
  void testEncoding() {
    var time = Instant.now();
    String taskId = "task-001";

    ExecutionTimeAndId boundary = new ExecutionTimeAndId(time, taskId);
    String encoded = boundary.toEncodedString();

    assertNotNull(encoded);
    assertFalse(encoded.isEmpty());

    ExecutionTimeAndId decoded = ExecutionTimeAndId.fromEncodedString(encoded);
    assertEquals(boundary, decoded);
  }

  @Test
  void testInvalidEncoding() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ExecutionTimeAndId.fromEncodedString("invalid-base64"));
    assertThrows(
        IllegalArgumentException.class, () -> ExecutionTimeAndId.fromEncodedString("aW52YWxpZA=="));
  }

  @Test
  void testEqualsAndHashCode() {
    var time = Instant.now();
    String taskId = "task-001";

    ExecutionTimeAndId boundary1 = new ExecutionTimeAndId(time, taskId);
    ExecutionTimeAndId boundary2 = new ExecutionTimeAndId(time, taskId);
    ExecutionTimeAndId boundary3 = new ExecutionTimeAndId(time.plusSeconds(1), taskId);

    assertEquals(boundary1, boundary2);
    assertEquals(boundary1.hashCode(), boundary2.hashCode());
    assertNotEquals(boundary1, boundary3);
  }
}
