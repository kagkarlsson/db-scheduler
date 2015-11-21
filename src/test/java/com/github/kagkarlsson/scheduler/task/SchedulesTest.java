package com.github.kagkarlsson.scheduler.task;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SchedulesTest {
	private static final LocalDateTime NOON = LocalDateTime.now().toLocalDate().atTime(12, 0);

	@Test
	public void should_validate_pattern() {
		assertIllegalArgument(null);
		assertIllegalArgument("");
		assertIllegalArgument("LALA|123s");

		assertIllegalArgument("DAILY|");
		assertIllegalArgument("DAILY|1200");
		assertIllegalArgument("DAILY|12:00;13:00");
		assertIllegalArgument("DAILY|12:00,13:00,");

		assertParsable("DAILY|12:00", Daily.class);
		Schedule dailySchedule = assertParsable("DAILY|12:00,13:00", Daily.class);
		assertThat(dailySchedule.getNextExecutionTime(NOON), is(NOON.plusHours(1)));

		assertIllegalArgument("FIXED_DELAY|");
		assertIllegalArgument("FIXED_DELAY|123");

		Schedule fixedDelaySchedule = assertParsable("FIXED_DELAY|10s", FixedDelay.class);
		assertThat(fixedDelaySchedule.getNextExecutionTime(NOON), is(NOON.plusSeconds(10)));
	}

	private Schedule assertParsable(String schedule, Class clazz) {
		Schedule parsed = Schedules.parseSchedule(schedule);
		assertThat(parsed, instanceOf(clazz));
		return parsed;
	}

	private void assertIllegalArgument(String schedule) {
		try {
			Schedules.parseSchedule(schedule);
			fail("Should have thrown IllegalArgument for schedule '" + schedule + "'");
		} catch (Schedules.UnrecognizableSchedule e) {
			assertThat(e.getMessage(), CoreMatchers.containsString("Unrecognized schedule"));
			return;
		}
	}
}