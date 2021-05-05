package com.github.kagkarlsson.examples;

import java.time.Duration;
import java.time.Instant;

import javax.sql.DataSource;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.examples.helpers.ExampleHelpers;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;

import static java.time.Duration.*;

public class ExponentialBackoffMain extends Example {

    public static void main(String[] args) {
        new ExponentialBackoffMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        OneTimeTask<Void> failingTask = Tasks.oneTime("exponential_backoff_task")
            .onFailure(new FailureHandler.ExponentialBackoffFailureHandler<>(ofSeconds(1)))
            .execute((taskInstance, executionContext) -> {
                throw new RuntimeException("simulated task exception");
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource, failingTask)
            .pollingInterval(ofSeconds(2))
            .build();

        scheduler.schedule(failingTask.instance("1"), Instant.now());

        ExampleHelpers.registerShutdownHook(scheduler);

        scheduler.start();
    }
}
