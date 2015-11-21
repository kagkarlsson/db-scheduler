package com.github.kagkarlsson.scheduler.task;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDate;
import java.time.LocalTime;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DailyTest {

	private static final LocalTime HOUR_0 = LocalTime.of(0, 0);
	private static final LocalTime HOUR_8 = LocalTime.of(8, 0);
	private static final LocalTime HOUR_12 = LocalTime.of(12, 0);

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void should_not_allow_empty_times() {
		expectedException.expect(IllegalArgumentException.class);
		new Daily(new LocalTime[]{});
	}

	@Test
	public void should_generate_next_date_correctly() {
		LocalDate currentDate = LocalDate.now();
		LocalDate nextDay = currentDate.plusDays(1);
		assertThat(new Daily(HOUR_8).getNextExecutionTime(currentDate.atTime(HOUR_0)), is(currentDate.atTime(HOUR_8)));
		assertThat(new Daily(HOUR_8).getNextExecutionTime(currentDate.atTime(HOUR_8)), is(nextDay.atTime(HOUR_8)));

		assertThat(new Daily(HOUR_8, HOUR_12).getNextExecutionTime(currentDate.atTime(HOUR_0)), is(currentDate.atTime(HOUR_8)));
		assertThat(new Daily(HOUR_8, HOUR_12).getNextExecutionTime(currentDate.atTime(HOUR_8)), is(currentDate.atTime(HOUR_12)));
		assertThat(new Daily(HOUR_8, HOUR_12).getNextExecutionTime(currentDate.atTime(HOUR_12)), is(nextDay.atTime(HOUR_8)));
		//order should be irrelevant
		assertThat(new Daily(HOUR_12, HOUR_8).getNextExecutionTime(currentDate.atTime(HOUR_0)), is(currentDate.atTime(HOUR_8)));
	}

}