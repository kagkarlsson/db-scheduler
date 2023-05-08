package com.github.kagkarlsson.scheduler.task;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.kagkarlsson.scheduler.task.helper.CronExpressionStyle;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CronTest {

  ZoneId london = ZoneId.of("Europe/London");
  ZoneId utc = ZoneId.of("UTC");
  ZoneId newYork = ZoneId.of("America/New_York");

  @Test
  public void should_validate_pattern() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          new CronSchedule("asdf asdf asdf");
        });
  }

  @Test
  public void should_validate_zone() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          new CronSchedule("0 * * * * ?", null);
        });
  }

  @Test
  public void should_generate_next_date_correctly() {
    ZonedDateTime timeDone = ZonedDateTime.of(2000, 1, 1, 12, 0, 0, 0, ZoneId.systemDefault());

    assertNextExecutionTime(timeDone, "0 * * * * ?", timeDone.plusMinutes(1));
    assertNextExecutionTime(timeDone, "0 0 * * * ?", timeDone.plusHours(1));
    assertNextExecutionTime(timeDone, "0 0 12 * * ?", timeDone.plusDays(1));
    assertNextExecutionTime(timeDone, "0 0 12 1 * ?", timeDone.plusMonths(1));
    assertNextExecutionTime(timeDone, "0 0 12 1 1 ?", timeDone.plusYears(1));
  }

  @Test
  public void should_generate_next_date_correctly_with_quartz_cron() {
    ZonedDateTime timeDone = ZonedDateTime.of(2000, 1, 1, 12, 0, 0, 0, ZoneId.systemDefault());

    assertNextExecutionTime(timeDone, "0 * * * * ? *", timeDone.plusMinutes(1), CronExpressionStyle.QUARTZ);
    assertNextExecutionTime(timeDone, "0 0 * * * ? *", timeDone.plusHours(1), CronExpressionStyle.QUARTZ);
    assertNextExecutionTime(timeDone, "0 0 12 * * ? *", timeDone.plusDays(1), CronExpressionStyle.QUARTZ);
    assertNextExecutionTime(timeDone, "0 0 12 1 * ? *", timeDone.plusMonths(1), CronExpressionStyle.QUARTZ);
    assertNextExecutionTime(timeDone, "0 0 12 1 1 ? 2005", timeDone.plusYears(5), CronExpressionStyle.QUARTZ);
  }

  @Test
  public void should_generate_next_date_correctly_with_unix_cron() {
    ZonedDateTime timeDone = ZonedDateTime.of(2000, 1, 1, 12, 0, 0, 0, ZoneId.systemDefault());
    assertNextExecutionTime(timeDone, "* * * * *", timeDone.plusMinutes(1), CronExpressionStyle.UNIX);
    assertNextExecutionTime(timeDone, "0 * * * *", timeDone.plusHours(1), CronExpressionStyle.UNIX);
    assertNextExecutionTime(timeDone, "0 12 * * *", timeDone.plusDays(1), CronExpressionStyle.UNIX);
    assertNextExecutionTime(timeDone, "0 12 1 * *", timeDone.plusMonths(1), CronExpressionStyle.UNIX);
    assertNextExecutionTime(timeDone, "0 12 1 1 *", timeDone.plusYears(1), CronExpressionStyle.UNIX);
  }

  @Test
  public void should_take_time_zone_into_account() {
    ZoneId london = ZoneId.of("Europe/London");

    ZonedDateTime timeDone =
        ZonedDateTime.of(
            2019, 10, 26, 12, 0, 0, 0,
            london); // British Summer time 2019 ends at 2am on Sunday 27th October

    // "0 0 12 * * ?" means: At 12:00:00pm every day
    assertNextExecutionTime(
        timeDone,
        "0 0 12 * * ?",
        london,
        timeDone.plusHours(25)); // 12 midday on the 27th is 25 hours after 12 midday on the 26th
  }

  @Test
  public void should_always_use_time_zone() {

    // 11am UTC = 12pm BST
    ZonedDateTime timeDone =
        ZonedDateTime.of(
            2019, 10, 26, 11, 0, 0, 0,
            utc); // British Summer time 2019 ends at 2am on Sunday 27th October

    // "0 0 12 * * ?" means: At 12:00:00pm every day
    assertNextExecutionTime(
        timeDone,
        "0 0 12 * * ?",
        london,
        timeDone.plusHours(25)); // 12 midday on the 27th is 25 hours after 12 midday on the 26th

    // Every day: 13:05 and 20:05 New York time
    ZonedDateTime firstJanuaryMiddayUTC =
        ZonedDateTime.of(2000, 1, 1, 12, 0, 0, 0, utc); // midday UTC = 07:00 New York time
    assertNextExecutionTime(
        firstJanuaryMiddayUTC,
        "0 05 13,20 * * ?",
        newYork,
        ZonedDateTime.of(
            2000, 1, 1, 13, 5, 0, 0, newYork)); // next fire time should be 13:05 New York time
  }

  @Test
  public void should_mark_schedule_as_disabled() {
    assertTrue(Schedules.cron("-").isDisabled());
    assertFalse(Schedules.cron("0 * * * * ?").isDisabled());
  }

  private void assertNextExecutionTime(
      ZonedDateTime timeDone, String cronPattern, ZonedDateTime expectedTime) {
    assertNextExecutionTime(timeDone, expectedTime, new CronSchedule(cronPattern));
  }

  private void assertNextExecutionTime(
      ZonedDateTime timeDone, String cronPattern, ZonedDateTime expectedTime, CronExpressionStyle cronType) {
    assertNextExecutionTime(
        timeDone, expectedTime, new CronSchedule(cronPattern, expectedTime.getZone(), cronType));
  }

  private void assertNextExecutionTime(
      ZonedDateTime timeDone, String cronPattern, ZoneId zoneId, ZonedDateTime expectedTime) {
    assertNextExecutionTime(timeDone, expectedTime, Schedules.cron(cronPattern, zoneId));
  }

  private void assertNextExecutionTime(
      ZonedDateTime timeDone, ZonedDateTime expectedTime, Schedule schedule) {
    Instant nextExecutionTime = schedule.getNextExecutionTime(complete(timeDone.toInstant()));

    assertThat(nextExecutionTime, is(expectedTime.toInstant()));
  }

  private ExecutionComplete complete(Instant timeDone) {
    return ExecutionComplete.success(null, timeDone, timeDone);
  }
}
