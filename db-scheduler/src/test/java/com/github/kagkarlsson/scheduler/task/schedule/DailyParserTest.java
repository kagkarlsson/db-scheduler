package com.github.kagkarlsson.scheduler.task.schedule;

import org.junit.Test;

import java.time.LocalTime;
import java.time.ZoneId;

import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertParsableSchedule;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertUnrecognizableSchedule;
import static org.junit.Assert.assertEquals;

public class DailyParserTest {
    private final static LocalTime HOUR_9_30 = LocalTime.of(9, 30);
    private final static LocalTime HOUR_16_30 = LocalTime.of(16, 30);
    private final static LocalTime HOUR_18_30 = LocalTime.of(18, 30);
    private final static LocalTime HOUR_20_30 = LocalTime.of(20, 30);
    private static final ZoneId ROME = ZoneId.of("Europe/Rome");

    private final DailyParser parser = new DailyParser();

    @Test
    public void should_fail_on_unrecognizable_schedule_string() {
        assertUnrecognizableSchedule(parser, null);
        assertUnrecognizableSchedule(parser,"");
        assertUnrecognizableSchedule(parser,"LALA|123s");
        assertUnrecognizableSchedule(parser,"DAILY|");
        assertUnrecognizableSchedule(parser,"DAILY|1200");
        assertUnrecognizableSchedule(parser,"DAILY|12:00;13:00");
        assertUnrecognizableSchedule(parser,"DAILY|12:00,13:00,");
        assertUnrecognizableSchedule(parser,"DAILY|UTC|12:00,13:00");
        assertUnrecognizableSchedule(parser,"DAILY|UTC");
        assertUnrecognizableSchedule(parser,"DAILY|1200|UTC");
        assertUnrecognizableSchedule(parser,"DAILY|12:00|UTC|");
        assertUnrecognizableSchedule(parser,"DAILY|12:00,13:00|");
        assertUnrecognizableSchedule(parser,"DAILY|12:00,13:00|WRONG");
        assertUnrecognizableSchedule(parser,"DAILY|12:00,13:00|WRONG|WRONG");
    }

    @Test
    public void should_recognize_correct_schedule_string_without_timezone() {
        assertEquals(new Daily(HOUR_9_30), parser.parse("DAILY|09:30"));
        assertEquals(new Daily(HOUR_9_30, HOUR_16_30), parser.parse("DAILY|09:30,16:30"));
        assertEquals(new Daily(HOUR_9_30, HOUR_16_30, HOUR_18_30), parser.parse("DAILY|09:30,16:30,18:30"));
        assertEquals(new Daily(HOUR_9_30, HOUR_16_30, HOUR_18_30, HOUR_20_30), parser.parse("DAILY|09:30,16:30,18:30,20:30"));
    }

    @Test
    public void should_recognize_correct_schedule_with_timezone_string() {
        assertEquals(new Daily(ROME, HOUR_9_30), parser.parse("DAILY|09:30|Europe/Rome"));
        assertEquals(new Daily(ROME, HOUR_9_30, HOUR_16_30), parser.parse("DAILY|09:30,16:30|Europe/Rome"));
        assertEquals(new Daily(ROME, HOUR_9_30, HOUR_16_30, HOUR_18_30), parser.parse("DAILY|09:30,16:30,18:30|Europe/Rome"));
        assertEquals(new Daily(ROME, HOUR_9_30, HOUR_16_30, HOUR_18_30, HOUR_20_30), parser.parse("DAILY|09:30,16:30,18:30,20:30|Europe/Rome"));
    }

    @Test
    public void examples_should_be_parsable() {
        parser.examples().forEach(it -> assertParsableSchedule(parser, it));
    }
}
