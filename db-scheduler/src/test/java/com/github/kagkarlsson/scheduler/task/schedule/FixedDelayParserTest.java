package com.github.kagkarlsson.scheduler.task.schedule;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertScheduleNotPresent;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertSchedulePresent;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class FixedDelayParserTest {
    private final FixedDelayParser parser = new FixedDelayParser();

	@Test
	public void should_fail_on_unrecognizable_schedule_string() {
        assertScheduleNotPresent(parser, null);
        assertScheduleNotPresent(parser,"");
        assertScheduleNotPresent(parser,"LALA|123s");
        assertScheduleNotPresent(parser,"FIXED_DELAY|");
        assertScheduleNotPresent(parser,"FIXED_DELAY|123");
	}

    @Test
    public void should_recognize_correct_schedule_string() {
        assertEquals(Optional.of(FixedDelay.ofSeconds(10)), parser.parse("FIXED_DELAY|10s"));
    }

    @Test
    public void examples_should_be_parsable() {
        parser.examples().forEach(it -> assertSchedulePresent(parser, it));
    }
}
