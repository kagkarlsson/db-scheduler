package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;

public class SpawningOtherTasksMain extends Example {

    public static void main(String[] args) {
        new SpawningOtherTasksMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        final OneTimeTask<Integer> printer =
            Tasks.oneTime("printer", Integer.class)
                .execute((taskInstance, executionContext) -> {
                    System.out.println("Printer: " + taskInstance.getData());
                });

        final RecurringTask<Void> spawner = Tasks.recurring("spawner", FixedDelay.ofSeconds(5))
            .execute((taskInstance, executionContext) -> {
                final SchedulerClient client = executionContext.getSchedulerClient();
                final long id = System.currentTimeMillis();

                System.out.println("Scheduling printer executions.");
                for (int i = 0; i < 5; i++) {
                    client.schedule(
                        printer.instance("print" + id + i, i),
                        Instant.now());
                }
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource, printer)
            .pollingInterval(Duration.ofSeconds(1))
            .startTasks(spawner)
            .registerShutdownHook()
            .build();

        // hourlyTask is automatically scheduled on startup if not already started (i.e. exists in the db)
        scheduler.start();
    }

}
