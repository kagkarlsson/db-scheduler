package com.github.kagkarlsson.scheduler.serializer.jackson;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.task.helper.PlainScheduleAndData;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.Daily;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JacksonSerializerTest {

    private JacksonSerializer serializer;

    @BeforeEach
    public void setUp() {
        serializer = new JacksonSerializer();
    }

    @Test
    public void serialize_instant() {
        final Instant now = Instant.now();
        assertEquals(now, serializer.deserialize(Instant.class, serializer.serialize(now)));
    }

    @Test
    public void serialize_cron() {
        CronSchedule cronSchedule = new CronSchedule("* * * * * *");
        assertEquals(
                cronSchedule.getPattern(),
                serializer
                        .deserialize(CronSchedule.class, serializer.serialize(cronSchedule))
                        .getPattern());
    }

    @Test
    public void serialize_daily() {
        Instant now = Instant.now();
        Daily daily = Schedules.daily(LocalTime.MIDNIGHT);
        Daily deserialized = serializer.deserialize(Daily.class, serializer.serialize(daily));
        assertEquals(daily.getInitialExecutionTime(now), deserialized.getInitialExecutionTime(now));
    }

    @Test
    public void serialize_fixed_delay() {
        Instant now = Instant.now();
        FixedDelay fixedDelay = Schedules.fixedDelay(Duration.ofHours(1));
        FixedDelay deserialized = serializer.deserialize(FixedDelay.class, serializer.serialize(fixedDelay));
        assertEquals(fixedDelay.getInitialExecutionTime(now), deserialized.getInitialExecutionTime(now));
    }

    @Test
    public void serialize_plain_schedule_and_data() {
        // Not recommended to use abstract types in data that is json-serialized, type-information is
        // destroyed
        Instant now = Instant.now();
        FixedDelay fixedDelay = Schedules.fixedDelay(Duration.ofHours(1));

        PlainScheduleAndData plainScheduleAndData = new PlainScheduleAndData(fixedDelay, 50);
        PlainScheduleAndData deserialized =
                serializer.deserialize(PlainScheduleAndData.class, serializer.serialize(plainScheduleAndData));

        assertEquals(
                plainScheduleAndData.getSchedule().getInitialExecutionTime(now),
                deserialized.getSchedule().getInitialExecutionTime(now));
        assertEquals(50, deserialized.getData());
    }

    @Test
    public void serialize_schedule_and_data() {
        Instant now = Instant.now();
        CronSchedule cronSchedule = new CronSchedule("* * * * * *");

        ScheduleAndDataForTest scheduleAndData = new ScheduleAndDataForTest(cronSchedule, 50L);
        ScheduleAndDataForTest deserialized =
                serializer.deserialize(ScheduleAndDataForTest.class, serializer.serialize(scheduleAndData));

        assertEquals(
                scheduleAndData.getSchedule().getInitialExecutionTime(now),
                deserialized.getSchedule().getInitialExecutionTime(now));
        assertEquals(50, deserialized.getData());
    }
}
