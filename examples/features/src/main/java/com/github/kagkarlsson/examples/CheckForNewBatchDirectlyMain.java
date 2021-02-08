package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.examples.helpers.ExampleHelpers;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;

import static com.github.kagkarlsson.examples.helpers.ExampleHelpers.sleep;

public class CheckForNewBatchDirectlyMain extends Example {

    public static void main(String[] args) {
        new CheckForNewBatchDirectlyMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        OneTimeTask<Void> onetimeTask = Tasks.oneTime("my_task")
            .execute((taskInstance, executionContext) -> {
                System.out.println("Executed!");
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource, onetimeTask)
            .pollingInterval(Duration.ofSeconds(10))
            .pollingLimit(4)
            .build();

        ExampleHelpers.registerShutdownHook(scheduler);

        scheduler.start();

        sleep(2);
        System.out.println("Scheduling 100 task-instances.");
        for (int i = 0; i < 100; i++) {
            scheduler.schedule(onetimeTask.instance(String.valueOf(i)), Instant.now());
        }

    }
}
