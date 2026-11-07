package com.github.kagkarlsson.scheduler.task;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.CronStyle;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

    assertNextExecutionTime(timeDone, "0 * * * * ? *", timeDone.plusMinutes(1), CronStyle.QUARTZ);
    assertNextExecutionTime(timeDone, "0 0 * * * ? *", timeDone.plusHours(1), CronStyle.QUARTZ);
    assertNextExecutionTime(timeDone, "0 0 12 * * ? *", timeDone.plusDays(1), CronStyle.QUARTZ);
    assertNextExecutionTime(timeDone, "0 0 12 1 * ? *", timeDone.plusMonths(1), CronStyle.QUARTZ);
    assertNextExecutionTime(timeDone, "0 0 12 1 1 ? 2005", timeDone.plusYears(5), CronStyle.QUARTZ);
  }

  @Test
  public void should_generate_next_date_correctly_with_unix_cron() {
    ZonedDateTime timeDone = ZonedDateTime.of(2000, 1, 1, 12, 0, 0, 0, ZoneId.systemDefault());
    assertNextExecutionTime(timeDone, "* * * * *", timeDone.plusMinutes(1), CronStyle.UNIX);
    assertNextExecutionTime(timeDone, "0 * * * *", timeDone.plusHours(1), CronStyle.UNIX);
    assertNextExecutionTime(timeDone, "0 12 * * *", timeDone.plusDays(1), CronStyle.UNIX);
    assertNextExecutionTime(timeDone, "0 12 1 * *", timeDone.plusMonths(1), CronStyle.UNIX);
    assertNextExecutionTime(timeDone, "0 12 1 1 *", timeDone.plusYears(1), CronStyle.UNIX);
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

  @Test
  public void should_not_fail_on_cronschedule_without_next_execution_time() {
    CronSchedule cron =
        new CronSchedule("0 0 0 29 2 MON#1", ZoneId.systemDefault(), CronStyle.SPRING53);
    assertEquals(Schedule.NEVER, cron.getNextExecutionTime(complete(Instant.now())));
  }

  @Test
  public void validate_cron_schedule_equals() {
    assertEquals(
        new CronSchedule("* * * * *", ZoneId.systemDefault(), CronStyle.UNIX),
        new CronSchedule("* * * * *", ZoneId.systemDefault(), CronStyle.UNIX));
    assertNotEquals(
        new CronSchedule("1 * * * *", ZoneId.systemDefault(), CronStyle.UNIX),
        new CronSchedule("* * * * *", ZoneId.systemDefault(), CronStyle.UNIX));
    assertNotEquals(
        new CronSchedule("1 * * * *", london, CronStyle.UNIX),
        new CronSchedule("1 * * * *", newYork, CronStyle.UNIX));
    assertNotEquals(
        new CronSchedule("* * * * * *", ZoneId.systemDefault(), CronStyle.SPRING53),
        new CronSchedule("* * * * * *", ZoneId.systemDefault(), CronStyle.SPRING));
  }

  @Test
  public void equals_and_hash_code() {
    EqualsVerifier.forClass(CronSchedule.class).verify();
  }

  @ParameterizedTest
  @CsvSource({
    "SPRING53, 0 30 1 * * ?",
    "SPRING, 0 30 1 * * ?",
    "QUARTZ, 0 30 1 * * ? *",
    "UNIX, 30 1 * * *",
    "CRON4J, 30 1 * * *"
  })
  public void should_run_a_time_repeated_when_the_clocks_go_back_once(
      CronStyle cronStyle, String pattern) {
    CronSchedule schedule = new CronSchedule(pattern, london, cronStyle);
    ZonedDateTime due = ZonedDateTime.of(2026, 10, 25, 1, 30, 0, 0, london); // the first 01:30

    assertThat(
        nextExecutionTime(schedule, due, due.plusSeconds(5)), is(due.plusDays(1).toInstant()));
  }

  @ParameterizedTest
  @CsvSource({
    "SPRING53, 0 30 1 * * ?",
    "SPRING, 0 30 1 * * ?",
    "QUARTZ, 0 30 1 * * ? *",
    "UNIX, 30 1 * * *",
    "CRON4J, 30 1 * * *"
  })
  public void should_run_a_time_skipped_when_the_clocks_go_forward_at_the_change(
      CronStyle cronStyle, String pattern) {
    CronSchedule schedule = new CronSchedule(pattern, london, cronStyle);
    ZonedDateTime due = ZonedDateTime.of(2026, 3, 28, 1, 30, 0, 0, london);
    ZonedDateTime change = ZonedDateTime.of(2026, 3, 29, 2, 0, 0, 0, london);

    assertThat(nextExecutionTime(schedule, due, due.plusSeconds(5)), is(change.toInstant()));
    assertThat(
        nextExecutionTime(schedule, change, change.plusSeconds(5)),
        is(ZonedDateTime.of(2026, 3, 30, 1, 30, 0, 0, london).toInstant()));
  }

  @Test
  public void should_run_crons_more_frequent_than_hourly_by_the_clock_when_the_clocks_change() {
    CronSchedule schedule = new CronSchedule("0 */15 1 * * ?", london);
    ZonedDateTime lastBeforeBack = ZonedDateTime.of(2026, 10, 25, 1, 45, 0, 0, london);
    ZonedDateTime lastBeforeForward = ZonedDateTime.of(2026, 3, 28, 1, 45, 0, 0, london);

    assertThat(
        nextExecutionTime(schedule, lastBeforeBack, lastBeforeBack.plusSeconds(5)),
        is(lastBeforeBack.plusMinutes(15).toInstant()));
    assertThat(
        nextExecutionTime(schedule, lastBeforeForward, lastBeforeForward.plusSeconds(5)),
        is(ZonedDateTime.of(2026, 3, 30, 1, 0, 0, 0, london).toInstant()));
  }

  @Test
  public void should_continue_from_time_done_when_execution_ran_past_next_time() {
    CronSchedule schedule = new CronSchedule("0 * * * * ?", utc);
    ZonedDateTime due = ZonedDateTime.of(2000, 1, 1, 12, 0, 0, 0, utc);

    assertThat(
        nextExecutionTime(schedule, due, due.plusSeconds(90)), is(due.plusMinutes(2).toInstant()));
  }

  private void assertNextExecutionTime(
      ZonedDateTime timeDone, String cronPattern, ZonedDateTime expectedTime) {
    assertNextExecutionTime(timeDone, expectedTime, new CronSchedule(cronPattern));
  }

  private void assertNextExecutionTime(
      ZonedDateTime timeDone, String cronPattern, ZonedDateTime expectedTime, CronStyle cronType) {
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

  private Instant nextExecutionTime(Schedule schedule, ZonedDateTime due, ZonedDateTime done) {
    return schedule.getNextExecutionTime(
        ExecutionComplete.success(
            new Execution(due.toInstant(), null), due.toInstant(), done.toInstant()));
  }
}
