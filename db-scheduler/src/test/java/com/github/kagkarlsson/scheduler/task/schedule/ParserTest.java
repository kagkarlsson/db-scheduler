package com.github.kagkarlsson.scheduler.task.schedule;

import org.junit.Test;

import java.util.Optional;

import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.ANY_SCHEDULE_STRING;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.DAILY;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.DAILY_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.THROWING_PARSER;
import static org.junit.Assert.assertEquals;

public class ParserTest {
    @Test
    public void returns_empty_optional_when_try_parsing_and_failure() {
        assertEquals(Optional.empty(), THROWING_PARSER.tryParse(ANY_SCHEDULE_STRING));
    }

    @Test
    public void returns_not_empty_optional_when_try_parsing_and_success() {
        assertEquals(Optional.of(DAILY), DAILY_PARSER.tryParse(ANY_SCHEDULE_STRING));
    }
}
