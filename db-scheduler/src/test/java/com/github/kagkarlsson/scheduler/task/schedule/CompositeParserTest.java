package com.github.kagkarlsson.scheduler.task.schedule;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.ANY_SCHEDULE_STRING;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.DAILY;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.DAILY_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.FIXED_DELAY;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.FIXED_DELAY_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.THROWING_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertParsableSchedule;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertUnrecognizableSchedule;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CompositeParserTest {

    @Test
    public void returns_schedule_from_the_first_successful_parser() {
        assertParsingResult(DAILY, DAILY_PARSER, FIXED_DELAY_PARSER, THROWING_PARSER);
        assertParsingResult(DAILY, THROWING_PARSER, DAILY_PARSER, FIXED_DELAY_PARSER);
        assertParsingResult(FIXED_DELAY, FIXED_DELAY_PARSER, DAILY_PARSER, THROWING_PARSER);
        assertParsingResult(FIXED_DELAY, THROWING_PARSER, FIXED_DELAY_PARSER, DAILY_PARSER);
    }

    @Test
    public void fails_when_no_successful_parsers() {
        assertUnrecognizableSchedule(CompositeParser.of(THROWING_PARSER), ANY_SCHEDULE_STRING);
    }

    @Test
    public void cannot_create_from_null_or_empty_list() {
        assertUnableToCreateParser(null);
        assertUnableToCreateParser();
    }

    @Test
    public void examples_should_be_parsable() {
        Parser parser = CompositeParser.of(DAILY_PARSER, FIXED_DELAY_PARSER);
        parser.examples().forEach(it -> assertParsableSchedule(parser, it));
    }

    @Test
    public void examples_should_come_from_delegates() {
        Parser parser1 = new ScheduleParsersHelper.FakeParser(() -> DAILY, "PARSABLE 1");
        Parser parser2 = new ScheduleParsersHelper.FakeParser(() -> DAILY, "PARSABLE 2");
        Parser parser = CompositeParser.of(parser1, parser2);
        assertThat(parser.examples(), containsInAnyOrder("PARSABLE 1", "PARSABLE 2"));
    }

    static void assertParsingResult(Schedule expected, Parser... parsers) {
        assertEquals(expected, CompositeParser.of(parsers).parse(ANY_SCHEDULE_STRING));
    }

    static void assertUnableToCreateParser(Parser... parsers) {
        try {
            CompositeParser.of(parsers);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), CoreMatchers.containsString("Unable to create CompositeParser"));
        }
    }
}
