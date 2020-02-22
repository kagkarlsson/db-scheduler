package com.github.kagkarlsson.scheduler.task.schedule;

import org.junit.Test;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Optional;

import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertScheduleNotPresent;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertSchedulePresent;
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
        assertScheduleNotPresent(parser, null);
        assertScheduleNotPresent(parser,"");
        assertScheduleNotPresent(parser,"LALA|123s");
        assertScheduleNotPresent(parser,"DAILY|");
        assertScheduleNotPresent(parser,"DAILY|1200");
        assertScheduleNotPresent(parser,"DAILY|12:00;13:00");
        assertScheduleNotPresent(parser,"DAILY|12:00,13:00,");
        assertScheduleNotPresent(parser,"DAILY|UTC|12:00,13:00");
        assertScheduleNotPresent(parser,"DAILY|UTC");
        assertScheduleNotPresent(parser,"DAILY|1200|UTC");
        assertScheduleNotPresent(parser,"DAILY|12:00|UTC|");
        assertScheduleNotPresent(parser,"DAILY|12:00,13:00|");
        assertScheduleNotPresent(parser,"DAILY|12:00,13:00|WRONG");
        assertScheduleNotPresent(parser,"DAILY|12:00,13:00|WRONG|WRONG");
    }

    @Test
    public void should_recognize_correct_schedule_string_without_timezone() {
        assertEquals(Optional.of(new Daily(HOUR_9_30)), parser.parse("DAILY|09:30"));
        assertEquals(Optional.of(new Daily(HOUR_9_30, HOUR_16_30)), parser.parse("DAILY|09:30,16:30"));
        assertEquals(Optional.of(new Daily(HOUR_9_30, HOUR_16_30, HOUR_18_30)), parser.parse("DAILY|09:30,16:30,18:30"));
        assertEquals(Optional.of(new Daily(HOUR_9_30, HOUR_16_30, HOUR_18_30, HOUR_20_30)), parser.parse("DAILY|09:30,16:30,18:30,20:30"));
    }

    @Test
    public void should_recognize_correct_schedule_with_timezone_string() {
        assertEquals(Optional.of(new Daily(ROME, HOUR_9_30)), parser.parse("DAILY|09:30|Europe/Rome"));
        assertEquals(Optional.of(new Daily(ROME, HOUR_9_30, HOUR_16_30)), parser.parse("DAILY|09:30,16:30|Europe/Rome"));
        assertEquals(Optional.of(new Daily(ROME, HOUR_9_30, HOUR_16_30, HOUR_18_30)), parser.parse("DAILY|09:30,16:30,18:30|Europe/Rome"));
        assertEquals(Optional.of(new Daily(ROME, HOUR_9_30, HOUR_16_30, HOUR_18_30, HOUR_20_30)), parser.parse("DAILY|09:30,16:30,18:30,20:30|Europe/Rome"));
    }

    @Test
    public void examples_should_be_parsable() {
        parser.examples().forEach(it -> assertSchedulePresent(parser, it));
    }
}
