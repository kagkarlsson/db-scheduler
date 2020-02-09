package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.schedule.Daily;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

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

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void should_not_allow_empty_times() {
        expectedException.expect(IllegalArgumentException.class);
        new Daily();
    }

    @Test
    public void should_not_allow_null_zone_id() {
        expectedException.expect(NullPointerException.class);
        new Daily(null, new LocalTime[]{LocalTime.MIDNIGHT});
    }

    @Test
    public void should_generate_next_date_correctly() {
        LocalDate currentDate = Instant.now().atZone(ZONE).toLocalDate();
        LocalDate nextDay = currentDate.plusDays(1);

        assertThat(new Daily(HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0))), is(instant(currentDate, HOUR_8)));
        assertThat(new Daily(HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_8))), is(instant(nextDay, HOUR_8)));

        assertThat(new Daily(HOUR_8, HOUR_12).getNextExecutionTime(complete(instant(currentDate, HOUR_0))), is(instant(currentDate, HOUR_8)));
        assertThat(new Daily(HOUR_8, HOUR_12).getNextExecutionTime(complete(instant(currentDate, HOUR_8))), is(instant(currentDate, HOUR_12)));
        assertThat(new Daily(HOUR_8, HOUR_12).getNextExecutionTime(complete(instant(currentDate, HOUR_12))), is(instant(nextDay, HOUR_8)));
        //order should be irrelevant
        assertThat(new Daily(HOUR_12, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0))), is(instant(currentDate, HOUR_8)));
    }

    @Test
    public void should_generate_next_date_correctly_during_standard_time() {
        // Europe/Rome is UTC+1 during standard time
        LocalDate currentDate = LocalDate.of(2020, 12, 12);
        LocalDate nextDay = currentDate.plusDays(1);

        assertThat(new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))), is(instant(currentDate, HOUR_7, UTC)));
        assertThat(new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_7, UTC))), is(instant(nextDay, HOUR_7, UTC)));
    }

    @Test
    public void should_generate_next_date_correctly_during_daylight_saving_time() {
        // Europe/Rome is UTC+2 during daylight time
        LocalDate currentDate = LocalDate.of(2020, 7, 12);
        LocalDate nextDay = currentDate.plusDays(1);

        assertThat(new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))), is(instant(currentDate, HOUR_6, UTC)));
        assertThat(new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_6, UTC))), is(instant(nextDay, HOUR_6, UTC)));
    }

    @Test
    public void should_generate_next_date_correctly_on_the_day_before_time_changes_to_daylight_saving() {
        // Europe/Rome is UTC+1 during standard time
        // Europe/Rome is UTC+2 during daylight saving time
        // Daylight saving time begins in Rome on 29 March 2020
        LocalDate currentDate = LocalDate.of(2020, 3, 28);
        LocalDate nextDay = currentDate.plusDays(1);

        assertThat(new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))), is(instant(currentDate, HOUR_7, UTC)));
        assertThat(new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_7, UTC))), is(instant(nextDay, HOUR_6, UTC)));
    }

    @Test
    public void should_generate_next_date_correctly_on_the_day_before_time_changes_to_standard() {
        // Europe/Rome is UTC+1 during standard time
        // Europe/Rome is UTC+2 during daylight saving time
        // Standard time begins in Rome on 25 October 2020
        LocalDate currentDate = LocalDate.of(2020, 10, 24);
        LocalDate nextDay = currentDate.plusDays(1);

        assertThat(new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))), is(instant(currentDate, HOUR_6, UTC)));
        assertThat(new Daily(ROME, HOUR_8).getNextExecutionTime(complete(instant(currentDate, HOUR_6, UTC))), is(instant(nextDay, HOUR_7, UTC)));
    }

    @Test
    public void should_generate_next_date_correctly_on_the_day_when_time_changes_to_daylight_saving() {
        // Europe/Rome is UTC+1 during standard time
        // Europe/Rome is UTC+2 during daylight saving time
        // Daylight saving begins on 29 March 2020 at 2AM
        LocalDate currentDate = LocalDate.of(2020, 3, 29);
        LocalDate nextDay = currentDate.plusDays(1);

        assertThat(new Daily(ROME, HOUR_2).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))), is(instant(currentDate, HOUR_1, UTC)));
        assertThat(new Daily(ROME, HOUR_3).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))), is(instant(currentDate, HOUR_1, UTC)));
    }

    @Test
    public void should_generate_next_date_correctly_on_the_day_when_time_changes_to_standard() {
        // Europe/Rome is UTC+1 during standard time
        // Europe/Rome is UTC+2 during daylight saving time
        // Standard saving begins on 25 October 2020 at 3AM
        LocalDate currentDate = LocalDate.of(2020, 10, 25);
        LocalDate previousDay = currentDate.minusDays(1);

        assertThat(new Daily(ROME, HOUR_2).getNextExecutionTime(complete(instant(previousDay, HOUR_23, UTC))), is(instant(currentDate, HOUR_0, UTC)));
        assertThat(new Daily(ROME, HOUR_3).getNextExecutionTime(complete(instant(currentDate, HOUR_0, UTC))), is(instant(currentDate, HOUR_2, UTC)));
    }

    @Test
    public void equals() {
        assertEquals(new Daily(ZoneId.of("UTC"), LocalTime.MIDNIGHT), new Daily(ZoneId.of("UTC"), LocalTime.MIDNIGHT));
        assertNotEquals(new Daily(ZoneId.of("UTC"), LocalTime.MIDNIGHT, LocalTime.NOON), new Daily(ZoneId.of("UTC"), LocalTime.MIDNIGHT));
        assertNotEquals(new Daily(ZoneId.of("Europe/London"), LocalTime.MIDNIGHT), new Daily(ZoneId.of("UTC"), LocalTime.MIDNIGHT));
    }

    @Test
    public void to_string() {
        assertEquals("Daily times=[01:00, 02:00], zone=Europe/Rome", new Daily(ZoneId.of("Europe/Rome"), HOUR_1, HOUR_2).toString());
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
