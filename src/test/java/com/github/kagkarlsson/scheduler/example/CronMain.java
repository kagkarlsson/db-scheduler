package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.HsqlTestDatabaseRule;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;

public class CronMain {
    private static final Logger LOG = LoggerFactory.getLogger(CronMain.class);

    private static void example(DataSource dataSource) {

        RecurringTask<Void> cronTask = Tasks.recurring("cron-task", Schedules.cron("*/3 * * * * ?"))
                .execute((taskInstance, executionContext) -> {
                    System.out.println(Instant.now().getEpochSecond() + "s  -  Cron-schedule!");
                });

        final Scheduler scheduler = Scheduler
                .create(dataSource)
                .startTasks(cronTask)
                .pollingInterval(Duration.ofSeconds(1))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal.");
            scheduler.stop();
        }));

        scheduler.start();
    }

    public static void main(String[] args) throws Throwable {
        try {
            final HsqlTestDatabaseRule hsqlRule = new HsqlTestDatabaseRule();
            hsqlRule.before();
            final DataSource dataSource = hsqlRule.getDataSource();

            example(dataSource);
        } catch (Exception e) {
            LOG.error("Error", e);
        }

    }

}
