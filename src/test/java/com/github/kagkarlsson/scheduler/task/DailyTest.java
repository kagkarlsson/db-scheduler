package com.github.kagkarlsson.scheduler.task;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DailyTest {

	private static final ZoneId ZONE = ZoneId.systemDefault();
	private static final LocalTime HOUR_0 = LocalTime.of(0, 0);
	private static final LocalTime HOUR_8 = LocalTime.of(8, 0);
	private static final LocalTime HOUR_12 = LocalTime.of(12, 0);

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void should_not_allow_empty_times() {
		expectedException.expect(IllegalArgumentException.class);
		new Daily();
	}

	@Test
	public void should_generate_next_date_correctly() {
		LocalDate currentDate = Instant.now().atZone(ZONE).toLocalDate();
		LocalDate nextDay = currentDate.plusDays(1);

		assertThat(new Daily(HOUR_8).getNextExecutionTime(instant(currentDate, HOUR_0)), is(instant(currentDate, HOUR_8)));
		assertThat(new Daily(HOUR_8).getNextExecutionTime(instant(currentDate, HOUR_8)), is(instant(nextDay, HOUR_8)));

		assertThat(new Daily(HOUR_8, HOUR_12).getNextExecutionTime(instant(currentDate, HOUR_0)), is(instant(currentDate, HOUR_8)));
		assertThat(new Daily(HOUR_8, HOUR_12).getNextExecutionTime(instant(currentDate, HOUR_8)), is(instant(currentDate, HOUR_12)));
		assertThat(new Daily(HOUR_8, HOUR_12).getNextExecutionTime(instant(currentDate, HOUR_12)), is(instant(nextDay, HOUR_8)));
		//order should be irrelevant
		assertThat(new Daily(HOUR_12, HOUR_8).getNextExecutionTime(instant(currentDate, HOUR_0)), is(instant(currentDate, HOUR_8)));
	}

	private Instant instant(LocalDate date, LocalTime time) {
		return ZonedDateTime.of(date, time, ZONE).toInstant();
	}

}