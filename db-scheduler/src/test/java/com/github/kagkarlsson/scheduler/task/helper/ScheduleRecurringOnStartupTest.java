package com.github.kagkarlsson.scheduler.task.helper;

import static co.unruly.matchers.OptionalMatchers.empty;
import static com.github.kagkarlsson.scheduler.task.helper.ScheduleRecurringOnStartup.differenceGreaterThan;
import static com.github.kagkarlsson.scheduler.task.schedule.Schedules.daily;
import static com.github.kagkarlsson.scheduler.task.schedule.Schedules.fixedDelay;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ScheduleRecurringOnStartupTest {

    private static final ZoneId ZONE = ZoneId.systemDefault();
    private static final LocalDate DATE = LocalDate.of(2018, 3, 1);
    private static final LocalTime TIME_INSTANT_A = LocalTime.of(8, 0);
    private static final Instant UPCOMING_INSTANT_A = ZonedDateTime.of(DATE, TIME_INSTANT_A, ZONE).toInstant();
    private static final Instant NOW = UPCOMING_INSTANT_A.minus(Duration.ofHours(1));

    private static final String INSTANCE = "instance";
    private static final TaskInstance<Void> TASK_INSTANCE = new TaskInstance<>("recurring-a", INSTANCE);
    private SettableClock clock;

    @BeforeEach
    void setUp() {
        clock = new SettableClock();
        clock.set(NOW);
    }

    @Test
    void checkForNewExecutionTime_no_new_execution_time_if_execution_imminent() {
        assertNotUpdatedExecutionTime(scheduled(NOW.plus(Duration.ofSeconds(1))), daily(TIME_INSTANT_A.plusMinutes(1)));
        assertUpdatedExecutionTime(scheduled(NOW.plus(Duration.ofSeconds(11))), daily(TIME_INSTANT_A.plusMinutes(1)));
    }

    @Test
    void checkForNewExecutionTime_updated_if_deterministic_schedule_change() {
        assertNotUpdatedExecutionTime(scheduled(UPCOMING_INSTANT_A), daily(TIME_INSTANT_A));
        assertNotUpdatedExecutionTime(scheduled(UPCOMING_INSTANT_A), daily(TIME_INSTANT_A.plus(Duration.ofMillis(20)))); // requires
                                                                                                                         // 1s
                                                                                                                         // diff
                                                                                                                         // between
                                                                                                                         // instants
        assertUpdatedExecutionTime(scheduled(UPCOMING_INSTANT_A), daily(TIME_INSTANT_A.plus(Duration.ofSeconds(20))));
        assertUpdatedExecutionTime(scheduled(UPCOMING_INSTANT_A), daily(TIME_INSTANT_A.minus(Duration.ofSeconds(20))));
    }

    @Test
    void checkForNewExecutionTime_updated_if_non_deterministic_schedule_change_to_sooner() {
        assertNotUpdatedExecutionTime(scheduled(UPCOMING_INSTANT_A), fixedDelay(Duration.ofDays(1)));
        assertUpdatedExecutionTime(scheduled(UPCOMING_INSTANT_A), fixedDelay(Duration.ofMinutes(1)));
    }

    private void assertUpdatedExecutionTime(ScheduledExecution<Object> scheduled, Schedule newSchedule) {
        final ScheduleRecurringOnStartup<Void> unit = new ScheduleRecurringOnStartup<>(INSTANCE, null, newSchedule);
        assertThat(unit.checkForNewExecutionTime(clock, TASK_INSTANCE, scheduled), not(empty()));
    }

    private void assertNotUpdatedExecutionTime(ScheduledExecution<Object> scheduled, Schedule newSchedule) {
        final ScheduleRecurringOnStartup<Void> unit = new ScheduleRecurringOnStartup<>(INSTANCE, null, newSchedule);
        assertThat(unit.checkForNewExecutionTime(clock, TASK_INSTANCE, scheduled), empty());
    }

    @Test
    void test_differenceGreaterThan() {
        assertTrue(differenceGreaterThan(clock.now(), clock.now().minusSeconds(10), Duration.ofSeconds(1)));
        assertTrue(differenceGreaterThan(clock.now(), clock.now().plusSeconds(10), Duration.ofSeconds(1)));

        assertFalse(differenceGreaterThan(clock.now(), clock.now().minusSeconds(10), Duration.ofSeconds(11)));
        assertFalse(differenceGreaterThan(clock.now(), clock.now().plusSeconds(10), Duration.ofSeconds(11)));
    }

    private ScheduledExecution<Object> scheduled(Instant executionTime) {
        return new ScheduledExecution<>(Object.class, new Execution(executionTime, TASK_INSTANCE));
    }
}
