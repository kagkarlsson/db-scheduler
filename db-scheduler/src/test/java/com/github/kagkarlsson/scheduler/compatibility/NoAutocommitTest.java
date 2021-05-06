package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;

import static co.unruly.matchers.OptionalMatchers.empty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@Tag("compatibility")
@Testcontainers
public class NoAutocommitTest {

    private static HikariDataSource noAutocommitingDatasource;
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer();

    @BeforeAll
    private static void initSchema() {
        final DriverDataSource datasource = new DriverDataSource(POSTGRES.getJdbcUrl(), "org.postgresql.Driver",
            new Properties(), POSTGRES.getUsername(), POSTGRES.getPassword());

        // init schema
        DbUtils.runSqlResource("/postgresql_tables.sql").accept(datasource);

        // Setup non auto-committing datasource
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSource(datasource);
        hikariConfig.setAutoCommit(false);
        noAutocommitingDatasource = new HikariDataSource(hikariConfig);
    }

    @AfterEach
    public void clearTables() {
        assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
            DbUtils.clearTables(noAutocommitingDatasource)
        );
    }

    @Test
    public void onstartup_should_always_commit() {
        final TestTasks.CountingHandler<Void> counter = new TestTasks.CountingHandler<>();
        final RecurringTask<Void> recurring1 = Tasks.recurring("recurring1", new FutureSchedule())
            .execute(counter);

        // Setup non auto-committing datasource
        Scheduler scheduler = Scheduler.create(noAutocommitingDatasource)
            .startTasks(recurring1)
            .schedulerName(new SchedulerName.Fixed("test"))
            .commitWhenAutocommitDisabled(false)
            .build();
        stopScheduler.register(scheduler);

        scheduler.start();

        final Optional<ScheduledExecution<Object>> scheduledExecution = scheduler.getScheduledExecution(recurring1.getDefaultTaskInstance());
        assertThat(scheduledExecution, not(empty()));
    }

    private static class FutureSchedule implements Schedule {
        @Override
        public Instant getNextExecutionTime(ExecutionComplete executionComplete) {
            return Instant.now().plusSeconds(60);
        }

        @Override
        public Instant getInitialExecutionTime(Instant now) {
            return now.plus(Duration.ofMinutes(1));
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }
    }
}
