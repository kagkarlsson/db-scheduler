package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.examples.helpers.ExampleHelpers;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;

public class CronMain extends Example {

    public static void main(String[] args) {
        new CronMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        Schedule cron = Schedules.cron("*/10 * * * * ?");
        RecurringTask<Void> cronTask = Tasks.recurring("cron-task", cron)
            .execute((taskInstance, executionContext) -> {
                System.out.println(Instant.now().getEpochSecond() + "s  -  Cron-schedule!");
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource)
            .startTasks(cronTask)
            .pollingInterval(Duration.ofSeconds(1))
            .build();

        ExampleHelpers.registerShutdownHook(scheduler);

        scheduler.start();
    }

}
