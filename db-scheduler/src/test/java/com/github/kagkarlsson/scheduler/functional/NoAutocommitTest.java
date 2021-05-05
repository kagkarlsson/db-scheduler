package com.github.kagkarlsson.scheduler.functional;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static co.unruly.matchers.OptionalMatchers.empty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

public class NoAutocommitTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();


    @Test
    public void onstartup_should_always_commit() {
        final TestTasks.CountingHandler<Void> counter = new TestTasks.CountingHandler<>();
        final RecurringTask<Void> recurring1 = Tasks.recurring("recurring1", new FutureSchedule())
            .execute(counter);

        // Setup non auto-committing datasource
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSource(postgres.getNonPooledDatasource());
        hikariConfig.setAutoCommit(false);
        HikariDataSource noAutocommitDatasource = new HikariDataSource(hikariConfig);

        Scheduler scheduler = Scheduler.create(noAutocommitDatasource)
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
