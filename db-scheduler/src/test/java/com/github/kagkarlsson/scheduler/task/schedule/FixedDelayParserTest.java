package com.github.kagkarlsson.scheduler.task.schedule;

import org.junit.Test;

import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertParsableSchedule;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertUnrecognizableSchedule;
import static org.junit.Assert.assertEquals;

public class FixedDelayParserTest {
    private final FixedDelayParser parser = new FixedDelayParser();

	@Test
	public void should_fail_on_unrecognizable_schedule_string() {
        assertUnrecognizableSchedule(parser, null);
        assertUnrecognizableSchedule(parser,"");
        assertUnrecognizableSchedule(parser,"LALA|123s");
        assertUnrecognizableSchedule(parser,"FIXED_DELAY|");
        assertUnrecognizableSchedule(parser,"FIXED_DELAY|123");
	}

    @Test
    public void should_recognize_correct_schedule_string() {
        assertEquals(FixedDelay.ofSeconds(10), parser.parse("FIXED_DELAY|10s"));
    }

    @Test
    public void examples_should_be_parsable() {
        parser.examples().forEach(it -> assertParsableSchedule(parser, it));
    }
}
