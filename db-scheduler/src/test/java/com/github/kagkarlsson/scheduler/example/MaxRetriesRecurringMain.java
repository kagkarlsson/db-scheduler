package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.HsqlTestDatabaseExtension;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.VoidExecutionHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;

public class MaxRetriesRecurringMain {
    private static final Logger LOG = LoggerFactory.getLogger(MaxRetriesRecurringMain.class);

    private static void example(DataSource dataSource) {

        final Schedule mainSchedule = Schedules.fixedDelay(Duration.ofSeconds(10));

        final RecurringTask<Void> failingRecurringTask = Tasks.recurring("max_retries_recurring_task", mainSchedule)
            .onFailure((executionComplete, executionOperations) -> {
                if (executionComplete.getExecution().consecutiveFailures >= 3) {
                    System.out.println("Execution has failed " + executionComplete.getExecution().consecutiveFailures
                        + " times. Resetting to next execution-time according to main schedule.");
                    executionOperations.rescheduleAndResetFailures(mainSchedule.getNextExecutionTime(executionComplete));
                } else {
                    // try again in 1 second (max 3 times)
                    System.out.println("Execution has failed " + executionComplete.getExecution().consecutiveFailures + " times. Trying again in a bit...");

                    executionOperations.reschedule(executionComplete, executionComplete.getTimeDone().plusSeconds(1));
                }
            })
            .execute((taskInstance, executionContext) -> {
                throw new RuntimeException("simulated task exception");
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource)
            .startTasks(failingRecurringTask)
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
            final HsqlTestDatabaseExtension hsqlRule = new HsqlTestDatabaseExtension();
            hsqlRule.beforeEach(null);

            final DataSource dataSource = hsqlRule.getDataSource();

            example(dataSource);
        } catch (Exception e) {
            LOG.error("Error", e);
        }

    }

}
