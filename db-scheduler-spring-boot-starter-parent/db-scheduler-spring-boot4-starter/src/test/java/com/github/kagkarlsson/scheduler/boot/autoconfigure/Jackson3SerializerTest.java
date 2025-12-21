package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
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

public class Jackson3SerializerTest {

  private Jackson3Serializer serializer;

  @BeforeEach
  public void setUp() {
    serializer = new Jackson3Serializer();
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
    byte[] serialize = serializer.serialize(daily);
    System.out.println(new String(serialize));
    Daily deserialized = serializer.deserialize(Daily.class, serialize);
    assertEquals(daily.getInitialExecutionTime(now), deserialized.getInitialExecutionTime(now));
  }

  @Test
  public void serialize_fixed_delay() {
    Instant now = Instant.now();
    FixedDelay fixedDelay = Schedules.fixedDelay(Duration.ofHours(1));
    byte[] serialize = serializer.serialize(fixedDelay);
    System.out.println(new String(serialize));
    FixedDelay deserialized = serializer.deserialize(FixedDelay.class, serialize);
    assertEquals(
        fixedDelay.getNextExecutionTime(ExecutionComplete.simulatedSuccess(now)),
        deserialized.getNextExecutionTime(ExecutionComplete.simulatedSuccess(now)));
  }

  @Test
  public void serialize_plain_schedule_and_data() {
    // Not recommended to use abstract types in data that is json-serialized, type-information is
    // destroyed
    Instant now = Instant.now();
    FixedDelay fixedDelay = Schedules.fixedDelay(Duration.ofHours(1));

    PlainScheduleAndData plainScheduleAndData = new PlainScheduleAndData(fixedDelay, 50);
    PlainScheduleAndData deserialized =
        serializer.deserialize(
            PlainScheduleAndData.class, serializer.serialize(plainScheduleAndData));

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
