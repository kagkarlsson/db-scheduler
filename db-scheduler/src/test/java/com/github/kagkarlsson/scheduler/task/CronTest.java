package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
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

    @Test
    public void should_validate_pattern() {
        expectedException.expect(IllegalArgumentException.class);
        new CronSchedule("asdf asdf asdf");
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

    private void assertNextExecutionTime(ZonedDateTime timeDone, String cronPattern, ZonedDateTime expectedTime) {
        CronSchedule schedule = new CronSchedule(cronPattern);
        Instant nextExecutionTime = schedule.getNextExecutionTime(complete(timeDone.toInstant()));

        assertThat(nextExecutionTime, is(expectedTime.toInstant()));
    }

    private ExecutionComplete complete(Instant timeDone) {
        return ExecutionComplete.success(null, timeDone, timeDone);
    }
}