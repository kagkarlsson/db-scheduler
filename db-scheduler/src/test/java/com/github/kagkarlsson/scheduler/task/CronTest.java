package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CronTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    ZoneId london = ZoneId.of("Europe/London");
    ZoneId utc = ZoneId.of("UTC");
    ZoneId newYork = ZoneId.of("America/New_York");


    @Test
    public void should_validate_pattern() {
        expectedException.expect(IllegalArgumentException.class);
        new CronSchedule("asdf asdf asdf");
    }

    @Test
    public void should_validate_zone() {
        expectedException.expect(IllegalArgumentException.class);
        new CronSchedule("0 * * * * ?", null);
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
    public void should_take_time_zone_into_account() {
        ZoneId london = ZoneId.of("Europe/London");

        ZonedDateTime timeDone = ZonedDateTime.of(2019, 10, 26, 12, 0, 0, 0, london);  //British Summer time 2019 ends at 2am on Sunday 27th October

        // "0 0 12 * * ?" means: At 12:00:00pm every day
        assertNextExecutionTime(timeDone, "0 0 12 * * ?", london, timeDone.plusHours(25));  //12 midday on the 27th is 25 hours after 12 midday on the 26th
    }

    @Test
    public void should_always_use_time_zone() {

        //11am UTC = 12pm BST
        ZonedDateTime timeDone = ZonedDateTime.of(2019, 10, 26, 11, 0, 0, 0, utc);  //British Summer time 2019 ends at 2am on Sunday 27th October

        // "0 0 12 * * ?" means: At 12:00:00pm every day
        assertNextExecutionTime(timeDone, "0 0 12 * * ?", london, timeDone.plusHours(25));  //12 midday on the 27th is 25 hours after 12 midday on the 26th

        //Every day: 13:05 and 20:05 New York time
        ZonedDateTime firstJanuaryMiddayUTC = ZonedDateTime.of(2000, 1, 1, 12, 0, 0, 0, utc);   //midday UTC = 07:00 New York time
        assertNextExecutionTime(firstJanuaryMiddayUTC, "0 05 13,20 * * ?", newYork, ZonedDateTime.of(2000, 1, 1, 13, 5, 0, 0, newYork));  //next fire time should be 13:05 New York time
    }

    private void assertNextExecutionTime(ZonedDateTime timeDone, String cronPattern, ZonedDateTime expectedTime) {
        assertNextExecutionTime(timeDone, expectedTime, new CronSchedule(cronPattern));
    }

    private void assertNextExecutionTime(ZonedDateTime timeDone, String cronPattern, ZoneId zoneId, ZonedDateTime expectedTime) {
        assertNextExecutionTime(timeDone, expectedTime, Schedules.cron(cronPattern, zoneId));
    }

    private void assertNextExecutionTime(ZonedDateTime timeDone, ZonedDateTime expectedTime, Schedule schedule) {
        Instant nextExecutionTime = schedule.getNextExecutionTime(complete(timeDone.toInstant()));

        assertThat(nextExecutionTime, is(expectedTime.toInstant()));
    }

    private ExecutionComplete complete(Instant timeDone) {
        return ExecutionComplete.success(null, timeDone, timeDone);
    }
}
