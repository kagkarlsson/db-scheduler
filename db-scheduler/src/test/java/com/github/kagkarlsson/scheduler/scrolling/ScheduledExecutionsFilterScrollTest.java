package com.github.kagkarlsson.scheduler.scrolling;

import static co.unruly.matchers.OptionalMatchers.contains;
import static com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter.all;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.kagkarlsson.scheduler.ExecutionTimeAndId;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class ScheduledExecutionsFilterScrollTest {

  private final Instant anInstant = Instant.now();

  @Test
  void testScrollingAndLimiting() {
    var afterBoundary = new ExecutionTimeAndId(anInstant, "task-after");
    var beforeBoundary = new ExecutionTimeAndId(anInstant.plusSeconds(10), "task-before");

    var filter = all().after(afterBoundary).before(beforeBoundary).limit(100);

    assertThat(filter.getAfterExecution(), contains(afterBoundary));
    assertThat(filter.getBeforeExecution(), contains(beforeBoundary));
    assertThat(filter.getLimit(), contains(100));
  }

  @Test
  void testValidation() {
    assertThrows(NullPointerException.class, () -> all().after(null));
    assertThrows(NullPointerException.class, () -> all().before(null));

    assertThrows(IllegalArgumentException.class, () -> all().limit(0));
    assertThrows(IllegalArgumentException.class, () -> all().limit(-1));
  }
}
