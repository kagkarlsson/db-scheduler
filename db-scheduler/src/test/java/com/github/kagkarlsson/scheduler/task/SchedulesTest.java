package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.schedule.Daily;
import com.github.kagkarlsson.scheduler.task.schedule.DisabledSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

public class SchedulesTest {
    private static final Instant NOON_TODAY = ZonedDateTime.now().withHour(12).withMinute(0).withSecond(0).withNano(0).toInstant();
    private static final Instant NOON_TODAY_ROME = ZonedDateTime.now(ZoneId.of("Europe/Rome")).withHour(12).withMinute(0).withSecond(0).withNano(0).toInstant();

    @Test
    public void should_validate_pattern() {
        assertIllegalArgument(null);
        assertIllegalArgument("");
        assertIllegalArgument("LALA|123s");

        assertIllegalArgument("DAILY|");
        assertIllegalArgument("DAILY|1200");
        assertIllegalArgument("DAILY|12:00;13:00");
        assertIllegalArgument("DAILY|12:00,13:00,");

        assertIllegalArgument("DAILY|UTC");
        assertIllegalArgument("DAILY|UTC1200");
        assertIllegalArgument("DAILY|UTC1200|");
        assertIllegalArgument("DAILY||12:00,13:00");
        assertIllegalArgument("DAILY|WRONG|12:00,13:00");
        assertIllegalArgument("DAILY|UTC|UTC|12:00,13:00");
        assertIllegalArgument("DAILY|UTC|UTC12:00,13:00");

        assertParsable("DAILY|12:00", Daily.class);
        Schedule dailySchedule = assertParsable("DAILY|12:00,13:00", Daily.class);
        assertThat(dailySchedule.getNextExecutionTime(complete(NOON_TODAY)), is(NOON_TODAY.plus(Duration.ofHours(1))));

        assertParsable("DAILY|12:00|Europe/Rome", Daily.class);
        Schedule dailyScheduleWithTimezone = assertParsable("DAILY|10:00,14:00|Europe/Rome", Daily.class);
        assertThat(dailyScheduleWithTimezone.getNextExecutionTime(complete(NOON_TODAY_ROME)), is(NOON_TODAY_ROME.plus(Duration.ofHours(2))));

        Schedule fixedDelaySchedule = assertParsable("FIXED_DELAY|10s", FixedDelay.class);
        assertThat(fixedDelaySchedule.getNextExecutionTime(complete(NOON_TODAY)), is(NOON_TODAY.plusSeconds(10)));

        assertParsable("-", DisabledSchedule.class);
    }

    private ExecutionComplete complete(Instant timeDone) {
        return ExecutionComplete.success(null, timeDone, timeDone);
    }

    @SuppressWarnings("rawtypes")
    private Schedule assertParsable(String schedule, Class clazz) {
        Schedule parsed = Schedules.parseSchedule(schedule);
        assertThat(parsed, instanceOf(clazz));
        return parsed;
    }

    private void assertIllegalArgument(String schedule) {
        try {
            Schedules.parseSchedule(schedule);
            fail("Should have thrown UnrecognizableSchedule for schedule '" + schedule + "'");
        } catch (Schedules.UnrecognizableSchedule e) {
            assertThat(e.getMessage(), CoreMatchers.containsString("Unrecognized schedule"));
        }
    }
}
