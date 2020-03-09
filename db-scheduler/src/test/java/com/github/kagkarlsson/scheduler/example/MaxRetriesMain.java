package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.HsqlTestDatabaseExtension;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;

public class MaxRetriesMain {
    private static final Logger LOG = LoggerFactory.getLogger(MaxRetriesMain.class);

    private static void example(DataSource dataSource) {

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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal.");
            scheduler.stop();
        }));

        scheduler.start();
    }

    public static void main(String[] args) throws Throwable {
        try {
            final HsqlTestDatabaseExtension hsqlRule = new HsqlTestDatabaseExtension();
            hsqlRule.beforeEach(null);

            final DataSource dataSource = hsqlRule.getDataSource();

            example(dataSource);
        } catch (Exception e) {
            LOG.error("Error", e);
        }

    }

}
