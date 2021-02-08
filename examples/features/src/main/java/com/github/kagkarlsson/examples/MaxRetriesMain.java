package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.examples.helpers.ExampleHelpers;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;

public class MaxRetriesMain extends Example {

    public static void main(String[] args) {
        new MaxRetriesMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        OneTimeTask<Void> failingTask = Tasks.oneTime("max_retries_task")
            .onFailure((ExecutionComplete executionComplete, ExecutionOperations<Void> executionOperations) -> {

                if (executionComplete.getExecution().consecutiveFailures > 3) {
                    System.out.println("Execution has failed " + executionComplete.getExecution().consecutiveFailures + " times. Cancelling execution.");
                    executionOperations.stop();
                } else {
                    // try again in 1 second
                    System.out.println("Execution has failed " + executionComplete.getExecution().consecutiveFailures + " times. Trying again in a bit...");
                    executionOperations.reschedule(executionComplete, Instant.now().plusSeconds(1));
                }
            })
            .execute((taskInstance, executionContext) -> {
                throw new RuntimeException("simulated task exception");
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource, failingTask)
            .pollingInterval(Duration.ofSeconds(2))
            .build();

        scheduler.schedule(failingTask.instance("1"), Instant.now());

        ExampleHelpers.registerShutdownHook(scheduler);

        scheduler.start();
    }

}
