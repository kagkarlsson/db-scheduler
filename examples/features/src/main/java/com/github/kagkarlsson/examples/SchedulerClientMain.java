package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;

import javax.sql.DataSource;
import java.time.Instant;

public class SchedulerClientMain extends Example {

    public static void main(String[] args) {
        new SchedulerClientMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        final OneTimeTask<Void> task = Tasks.oneTime("task-a")
            .execute((taskInstance, executionContext) -> {
                System.out.println("Task a executed");
            });

        final SchedulerClient client = SchedulerClient.Builder.create(dataSource, task).build();

        final Instant now = Instant.now();
        for (int i = 0; i < 5; i++) {
            client.schedule(task.instance("id" + i), now.plusSeconds(i));
        }

        System.out.println("Listing scheduled executions");
        client.getScheduledExecutions(ScheduledExecutionsFilter.all())
            .forEach(execution -> {
                System.out.printf(
                    "Scheduled execution: taskName=%s, instance=%s, executionTime=%s%n",
                    execution.getTaskInstance().getTaskName(),
                    execution.getTaskInstance().getId(),
                    execution.getExecutionTime());
            });

    }
}
