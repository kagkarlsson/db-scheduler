package com.github.kagkarlsson.scheduler.task.schedule;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Optional;

import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.ANY_SCHEDULE_STRING;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.DAILY;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.DAILY_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.FIXED_DELAY;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.FIXED_DELAY_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.THROWING_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertScheduleNotPresent;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertSchedulePresent;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertUnrecognizableSchedule;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

public class NotThrowingParserTest {

    @Test
    public void returns_schedule_from_the_delegate() {
        assertParsingResult(DAILY, DAILY_PARSER);
        assertParsingResult(FIXED_DELAY, FIXED_DELAY_PARSER);
    }

    @Test
    public void does_not_fail_when_delegate_throws_exception() {
        assertScheduleNotPresent(NotThrowingParser.of(THROWING_PARSER), ANY_SCHEDULE_STRING);
    }

    @Test
    public void cannot_create_from_null_or_empty_list() {
        assertUnableToCreateParser(null);
    }

    @Test
    public void examples_should_be_parsable() {
        Parser parser = CompositeParser.of(DAILY_PARSER, FIXED_DELAY_PARSER);
        parser.examples().forEach(it -> assertSchedulePresent(parser, it));
    }

    @Test
    public void examples_should_come_from_delegates() {
        Parser parser1 = new ScheduleParsersHelper.FakeParser(() -> DAILY, "PARSABLE 1");
        Parser parser2 = new ScheduleParsersHelper.FakeParser(() -> DAILY, "PARSABLE 2");
        Parser parser = CompositeParser.of(parser1, parser2);
        assertThat(parser.examples(), containsInAnyOrder("PARSABLE 1", "PARSABLE 2"));
    }

    static void assertParsingResult(Schedule expected, Parser delegate) {
        assertEquals(Optional.of(expected), NotThrowingParser.of(delegate).parse(ANY_SCHEDULE_STRING));
    }

    static void assertUnableToCreateParser(Parser delegate) {
        try {
            NotThrowingParser.of(delegate);
            fail("Should have thrown IllegalArgumentException");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), CoreMatchers.containsString("Delegate parser cannot be null"));
        }
    }
}
