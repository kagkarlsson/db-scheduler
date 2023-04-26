package com.github.kagkarlsson.scheduler.serializer.gson;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.serializer.GsonSerializer;
import com.github.kagkarlsson.scheduler.serializer.jackson.ScheduleAndDataForTest;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.Daily;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GsonSerializerTest {

    private GsonSerializer serializer;

    @BeforeEach
    public void setUp() {
        serializer = new GsonSerializer();
    }

    @Test
    public void serialize_instant() {
        final Instant now = Instant.now();

        assertEquals(now, serializer.deserialize(Instant.class, serializer.serialize(now)));
    }

    @Test
    public void serialize_cron() {
        CronSchedule cronSchedule = new CronSchedule("* * * * * *");
        assertEquals(cronSchedule.getPattern(),
                serializer.deserialize(CronSchedule.class, serializer.serialize(cronSchedule)).getPattern());
    }

    @Test
    public void serialize_daily() {
        Instant now = Instant.now();
        ExecutionComplete executionComplete = ExecutionComplete.simulatedSuccess(now);

        Daily daily = Schedules.daily(LocalTime.MIDNIGHT);
        Daily deserialized = serializer.deserialize(Daily.class, serializer.serialize(daily));
        assertEquals(daily.getNextExecutionTime(executionComplete),
                deserialized.getNextExecutionTime(executionComplete));
    }

    @Test
    public void serialize_fixed_delay() {
        Instant now = Instant.now();
        ExecutionComplete executionComplete = ExecutionComplete.simulatedSuccess(now);

        FixedDelay fixedDelay = Schedules.fixedDelay(Duration.ofHours(1));
        FixedDelay deserialized = serializer.deserialize(FixedDelay.class, serializer.serialize(fixedDelay));

        assertEquals(fixedDelay.getNextExecutionTime(executionComplete),
                deserialized.getNextExecutionTime(executionComplete));
    }

    @Test
    public void serialize_schedule_and_data() {
        Instant now = Instant.now();
        CronSchedule cronSchedule = new CronSchedule("* * * * * *");

        ScheduleAndDataForTest scheduleAndData = new ScheduleAndDataForTest(cronSchedule, 50L);
        ScheduleAndDataForTest deserialized = serializer.deserialize(ScheduleAndDataForTest.class,
                serializer.serialize(scheduleAndData));

        assertEquals(scheduleAndData.getSchedule().getInitialExecutionTime(now),
                deserialized.getSchedule().getInitialExecutionTime(now));
        assertEquals(50, deserialized.getData());
    }

    private static class CustomData {
        private int id;

        public CustomData(int id) {

            this.id = id;
        }
    }
}
