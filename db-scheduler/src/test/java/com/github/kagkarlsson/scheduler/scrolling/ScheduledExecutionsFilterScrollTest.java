package com.github.kagkarlsson.scheduler.scrolling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.kagkarlsson.scheduler.ExecutionTimeAndId;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class ScheduledExecutionsFilterScrollTest {

  @Test
  void testScrollingAndLimiting() {
    Instant time = Instant.now();
    ExecutionTimeAndId afterBoundary = new ExecutionTimeAndId(time, "task-after");
    ExecutionTimeAndId beforeBoundary = new ExecutionTimeAndId(time.plusSeconds(10), "task-before");

    ScheduledExecutionsFilter filter =
        ScheduledExecutionsFilter.all().after(afterBoundary).before(beforeBoundary).limit(100);

    assertEquals(afterBoundary, filter.getAfterExecution().get());
    assertEquals(beforeBoundary, filter.getBeforeExecution().get());
    assertEquals(100, filter.getLimit().get());

    filter = ScheduledExecutionsFilter.all().withPicked(false).withIncludeUnresolved(true);

    ScheduledExecutionsFilter finalFilter = filter;
    assertThrows(NullPointerException.class, () -> finalFilter.after(null));
    assertThrows(NullPointerException.class, () -> finalFilter.before(null));

    assertThrows(IllegalArgumentException.class, () -> finalFilter.limit(0));
    assertThrows(IllegalArgumentException.class, () -> finalFilter.limit(-1));
  }
}
