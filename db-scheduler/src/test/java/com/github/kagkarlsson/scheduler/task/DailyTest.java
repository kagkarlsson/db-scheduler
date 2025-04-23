package com.github.kagkarlsson.scheduler.task;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.task.schedule.Daily;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DailyTest {

  private static final ZoneId ZONE = ZoneId.systemDefault();
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final ZoneId ROME = ZoneId.of("Europe/Rome");

  private static final LocalTime HOUR_0 = LocalTime.of(0, 0);
  private static final LocalTime HOUR_1 = LocalTime.of(1, 0);
  private static final LocalTime HOUR_2 = LocalTime.of(2, 0);
  private static final LocalTime HOUR_3 = LocalTime.of(3, 0);
  private static final LocalTime HOUR_6 = LocalTime.of(6, 0);
  private static final LocalTime HOUR_7 = LocalTime.of(7, 0);
  private static final LocalTime HOUR_8 = LocalTime.of(8, 0);
  private static final LocalTime HOUR_12 = LocalTime.of(12, 0);
  private static final LocalTime HOUR_23 = LocalTime.of(23, 0);

  @Test
  public void should_not_allow_empty_times() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          new Daily();
        });
  }

  @Test
  public void should_not_allow_null_zone_id() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          new Daily(null, new LocalTime[] {LocalTime.MIDNIGHT});
        });
  }

  @Test
  public void should_generate_next_date_correctly() {
    LocalDate currentDate = Instant.now().atZone(ZONE).toLocalDate();
    LocalDate nextDay = currentDate.plusDays(1);

    assertThat(
        new Daily(HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0))),
        is(instant(currentDate, HOUR_8)));
    assertThat(
        new Daily(HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_8))),
        is(instant(nextDay, HOUR_8)));

    assertThat(
        new Daily(HOUR_8, HOUR_12).getNextExecutionTime(complete(instant(currentDate, HOUR_0))),
        is(instant(currentDate, HOUR_8)));
    assertThat(
        new Daily(HOUR_8, HOUR_12).getNextExecutionTime(complete(instant(currentDate, HOUR_8))),
        is(instant(currentDate, HOUR_12)));
    assertThat(
        new Daily(HOUR_8, HOUR_12).getNextExecutionTime(complete(instant(currentDate, HOUR_12))),
        is(instant(nextDay, HOUR_8)));
    // order should be irrelevant
    assertThat(
        new Daily(HOUR_12, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0))),
        is(instant(currentDate, HOUR_8)));
  }

  @Test
  public void should_generate_next_date_correctly_during_standard_time() {
    // Europe/Rome is UTC+1 during standard time
    LocalDate currentDate = LocalDate.of(2020, 12, 12);
    LocalDate nextDay = currentDate.plusDays(1);

    assertThat(
        new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))),
        is(instant(currentDate, HOUR_7, UTC)));
    assertThat(
        new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_7, UTC))),
        is(instant(nextDay, HOUR_7, UTC)));
  }

  @Test
  public void should_generate_next_date_correctly_during_daylight_saving_time() {
    // Europe/Rome is UTC+2 during daylight time
    LocalDate currentDate = LocalDate.of(2020, 7, 12);
    LocalDate nextDay = currentDate.plusDays(1);

    assertThat(
        new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))),
        is(instant(currentDate, HOUR_6, UTC)));
    assertThat(
        new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_6, UTC))),
        is(instant(nextDay, HOUR_6, UTC)));
  }

  @Test
  public void
      should_generate_next_date_correctly_on_the_day_before_time_changes_to_daylight_saving() {
    // Europe/Rome is UTC+1 during standard time
    // Europe/Rome is UTC+2 during daylight saving time
    // Daylight saving time begins in Rome on 29 March 2020
    LocalDate currentDate = LocalDate.of(2020, 3, 28);
    LocalDate nextDay = currentDate.plusDays(1);

    assertThat(
        new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))),
        is(instant(currentDate, HOUR_7, UTC)));
    assertThat(
        new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_7, UTC))),
        is(instant(nextDay, HOUR_6, UTC)));
  }

  @Test
  public void should_generate_next_date_correctly_on_the_day_before_time_changes_to_standard() {
    // Europe/Rome is UTC+1 during standard time
    // Europe/Rome is UTC+2 during daylight saving time
    // Standard time begins in Rome on 25 October 2020
    LocalDate currentDate = LocalDate.of(2020, 10, 24);
    LocalDate nextDay = currentDate.plusDays(1);

    assertThat(
        new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))),
        is(instant(currentDate, HOUR_6, UTC)));
    assertThat(
        new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_6, UTC))),
        is(instant(nextDay, HOUR_7, UTC)));
  }

  @Test
  public void
      should_generate_next_date_correctly_on_the_day_when_time_changes_to_daylight_saving() {
    // Europe/Rome is UTC+1 during standard time
    // Europe/Rome is UTC+2 during daylight saving time
    // Daylight saving begins on 29 March 2020 at 2AM
    LocalDate currentDate = LocalDate.of(2020, 3, 29);

    assertThat(
        new Daily(ROME, HOUR_2).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))),
        is(instant(currentDate, HOUR_1, UTC)));
    assertThat(
        new Daily(ROME, HOUR_3).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))),
        is(instant(currentDate, HOUR_1, UTC)));
  }

  @Test
  public void should_generate_next_date_correctly_on_the_day_when_time_changes_to_standard() {
    // Europe/Rome is UTC+1 during standard time
    // Europe/Rome is UTC+2 during daylight saving time
    // Standard saving begins on 25 October 2020 at 3AM
    LocalDate currentDate = LocalDate.of(2020, 10, 25);
    LocalDate previousDay = currentDate.minusDays(1);

    assertThat(
        new Daily(ROME, HOUR_2).getNextExecutionTime(complete(instant(previousDay, HOUR_23, UTC))),
        is(instant(currentDate, HOUR_0, UTC)));
    assertThat(
        new Daily(ROME, HOUR_3).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))),
        is(instant(currentDate, HOUR_2, UTC)));
  }

  @Test
  public void
      should_generate_next_date_correctly_on_the_day_when_time_changes_to_standard_exists_twice_should_run_once() {
    // Europe/Rome is UTC+1 during standard time
    // Europe/Rome is UTC+2 during daylight saving time
    // Standard time begins in Rome on 25 October 2020
    LocalDate currentDate = LocalDate.of(2020, 10, 25);
    LocalDate previousDate = currentDate.minusDays(1);
    LocalDate nextDate = currentDate.plusDays(1);

    // 02:00 appear twice in Rome at 2020-10-25, one at 00:00 UTC, one at 01:00 UTC
    Daily dailyAt2 = new Daily(ROME, HOUR_2);
    // expect the instance with earlier offset (02:00 UTC+2)
    Instant expectedCurrentDateExecutionTime = instant(currentDate, HOUR_0, UTC);
    assertThat(expectedCurrentDateExecutionTime, is(instant(currentDate, HOUR_2, ROME)));
    // next execution time should be the day after with new offset (UTC+1)
    Instant expectedNextDateExecutionTime = instant(nextDate, HOUR_1, UTC);
    assertThat(expectedNextDateExecutionTime, is(instant(nextDate, HOUR_2, ROME)));

    assertThat(
        dailyAt2.getNextExecutionTime(complete(instant(previousDate, HOUR_0, UTC))),
        is(expectedCurrentDateExecutionTime));
    assertThat(
        dailyAt2.getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))),
        is(expectedNextDateExecutionTime));
  }

  @Test
  public void
      should_generate_next_date_correctly_on_the_day_when_time_changes_to_daylight_saving_time_doesnt_exists_should_run_later_then_recover() {
    // Europe/Rome is UTC+1 during standard time
    // Europe/Rome is UTC+2 during daylight saving time
    // Daylight saving begins on 29 March 2020 at 2AM
    LocalDate currentDate = LocalDate.of(2020, 3, 29);
    LocalDate previousDate = currentDate.minusDays(1);
    LocalDate nextDate = currentDate.plusDays(1);

    // 02:00 never appear in Rome at 2020-03-29, because after 01:59 UTC+1, we have 03:00 UTC+2
    Daily dailyAtTwo = new Daily(ROME, HOUR_2);

    // expect the instance with earlier offset (02:00 UTC+1, which is 03:00 UTC+2 or 01:00 UTC+0)
    Instant expectedCurrentDateExecutionTime = instant(currentDate, HOUR_1, UTC);
    assertThat(expectedCurrentDateExecutionTime, is(instant(currentDate, HOUR_2, ROME)));

    // next execution time should be the day after with new offset (UTC+2)
    Instant expectedNextDateExecutionTime = instant(nextDate, HOUR_0, UTC);
    assertThat(expectedNextDateExecutionTime, is(instant(nextDate, HOUR_2, ROME)));

    assertThat(
        dailyAtTwo.getNextExecutionTime(complete(instant(previousDate, HOUR_2, ROME))),
        is(expectedCurrentDateExecutionTime));
    assertThat(
        dailyAtTwo.getNextExecutionTime(complete(instant(currentDate, HOUR_2, ROME))),
        is(expectedNextDateExecutionTime));
  }

  @Test
  public void equals_and_hash_code() {
    EqualsVerifier.forClass(Daily.class).verify();
  }

  @Test
  public void to_string() {
    assertEquals(
        "Daily times=[01:00, 02:00], zone=Europe/Rome",
        new Daily(ZoneId.of("Europe/Rome"), HOUR_1, HOUR_2).toString());
  }

  private Instant instant(LocalDate date, LocalTime time) {
    return instant(date, time, ZONE);
  }

  private Instant instant(LocalDate date, LocalTime time, ZoneId zone) {
    return ZonedDateTime.of(date, time, zone).toInstant();
  }

  private ExecutionComplete complete(Instant timeDone) {
    return ExecutionComplete.success(null, timeDone, timeDone);
  }
}
