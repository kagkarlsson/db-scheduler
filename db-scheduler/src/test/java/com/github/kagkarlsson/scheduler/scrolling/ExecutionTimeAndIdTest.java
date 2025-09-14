package com.github.kagkarlsson.scheduler.scrolling;

import static com.github.kagkarlsson.scheduler.ExecutionTimeAndId.fromEncodedString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.kagkarlsson.scheduler.ExecutionTimeAndId;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ExecutionTimeAndIdTest {

  private Instant anInstant = Instant.now();
  private String aTaskId = "task-001";

  @Test
  void testNullValidation() {
    assertThrows(NullPointerException.class, () -> new ExecutionTimeAndId(null, aTaskId));
    assertThrows(NullPointerException.class, () -> new ExecutionTimeAndId(anInstant, null));
  }

  @Test
  void testEncoding() {
    ExecutionTimeAndId boundary = new ExecutionTimeAndId(anInstant, aTaskId);
    assertEquals(boundary, fromEncodedString(boundary.toEncodedString()));
  }

  @Test
  void testInvalidEncoding() {
    assertThrowsInvalidEncoding("invalid-base64");
    assertThrowsInvalidEncoding("aW52YWxpZA==");
  }

  @Test
  void testEqualsAndHashCode() {
    ExecutionTimeAndId boundary1 = new ExecutionTimeAndId(anInstant, aTaskId);
    ExecutionTimeAndId boundary2 = new ExecutionTimeAndId(anInstant, aTaskId);
    ExecutionTimeAndId boundary3 = new ExecutionTimeAndId(anInstant.plusSeconds(1), aTaskId);

    assertEquals(boundary1, boundary2);
    assertNotEquals(boundary1, boundary3);

    assertEquals(boundary1.hashCode(), boundary2.hashCode());
  }

  private void assertThrowsInvalidEncoding(String encoded) {
    assertThrows(
      IllegalArgumentException.class,
      () -> fromEncodedString(encoded));
  }
}
