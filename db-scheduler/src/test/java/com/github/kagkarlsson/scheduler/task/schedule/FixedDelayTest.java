package com.github.kagkarlsson.scheduler.task.schedule;

import static org.junit.jupiter.api.Assertions.assertEquals;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

public class FixedDelayTest {
    @Test
    public void equals_and_hash_code() {
        EqualsVerifier.forClass(FixedDelay.class).verify();
    }

    @Test
    public void to_string() {
        assertEquals("FixedDelay duration=PT1.5S", FixedDelay.ofMillis(1500).toString());
        assertEquals("FixedDelay duration=PT3S", FixedDelay.ofSeconds(3).toString());
        assertEquals("FixedDelay duration=PT6M", FixedDelay.ofMinutes(6).toString());
        assertEquals("FixedDelay duration=PT12H", FixedDelay.ofHours(12).toString());
    }
}
