package com.github.kagkarlsson.scheduler.functional;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlRule;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static co.unruly.matchers.OptionalMatchers.contains;
import static org.junit.Assert.*;

public class DeleteUnresolvedTest {

	public static final ZoneId ZONE = ZoneId.systemDefault();
	private static final LocalDate DATE = LocalDate.of(2018, 3, 1);
	private static final LocalTime TIME = LocalTime.of(8, 0);
	private SettableClock clock;

	@Rule
	public EmbeddedPostgresqlRule postgres = new EmbeddedPostgresqlRule(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);


	@Before
	public void setUp() {
		clock = new SettableClock();
		clock.set(ZonedDateTime.of(DATE, TIME, ZONE).toInstant());
	}

	@Test
	public void should_delete_executions_with_old_unresolved_tasknames() {

        OneTimeTask<Void> onetime = Tasks.oneTime("onetime").execute(TestTasks.DO_NOTHING);


        TestableRegistry testableRegistry = new TestableRegistry(false, Collections.emptyList());
        // Missing task with name 'onetime'
        ManualScheduler scheduler = TestHelper.createManualScheduler(postgres.getDataSource())
				.clock(clock)
                .statsRegistry(testableRegistry)
				.build();

		scheduler.schedule(onetime.instance("id1"), clock.now());
        assertEquals(0, testableRegistry.getCount(StatsRegistry.SchedulerStatsEvent.UNRESOLVED_TASK));

        scheduler.runAnyDueExecutions();
        assertEquals(1, testableRegistry.getCount(StatsRegistry.SchedulerStatsEvent.UNRESOLVED_TASK));

        assertEquals(1, DbUtils.countExecutions(postgres.getDataSource()));

        scheduler.runDeadExecutionDetection();
        assertEquals(1, DbUtils.countExecutions(postgres.getDataSource()));

        clock.set(clock.now().plus(Duration.ofDays(30)));
        scheduler.runDeadExecutionDetection();
        assertEquals(0, DbUtils.countExecutions(postgres.getDataSource()));

        scheduler.runDeadExecutionDetection();
	}

}
