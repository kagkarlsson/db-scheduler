package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;

import javax.sql.DataSource;

public class RecurringTaskMain extends Example {

    public static void main(String[] args) {
        new RecurringTaskMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        RecurringTask<Void> hourlyTask = Tasks.recurring("my-hourly-task", FixedDelay.ofHours(1))
            .execute((inst, ctx) -> {
                System.out.println("Executed!");
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource)
            .startTasks(hourlyTask)
            .registerShutdownHook()
            .threads(5)
            .build();

        // hourlyTask is automatically scheduled on startup if not already started (i.e. exists in the db)
        scheduler.start();
    }
}
