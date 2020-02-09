package com.github.kagkarlsson.scheduler.task.schedule;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FixedDelayTest {
    @Test
    public void equals() {
        assertEquals(FixedDelay.of(Duration.ofMinutes(10)), FixedDelay.ofMinutes(10));
        assertNotEquals(FixedDelay.of(Duration.ofMinutes(10)), FixedDelay.ofMinutes(15));
    }

    @Test
    public void to_string() {
        assertEquals("FixedDelay duration=PT2M", FixedDelay.of(Duration.ofMinutes(2)).toString());
    }
}
