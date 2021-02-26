package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.examples.helpers.ExampleHelpers;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.IntStream;

public class DeletingUnresolvedTasksMain extends Example {

    public static void main(String[] args) {
        new DeletingUnresolvedTasksMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        RecurringTask<Void> unresolvedTask = Tasks.recurring("unresolved1", Schedules.fixedDelay(Duration.ofSeconds(1)))
                .execute((taskInstance, executionContext) -> {
                    System.out.println("Ran");
                });
        RecurringTask<Void> unresolvedTask2 = Tasks.recurring("unresolved2", Schedules.fixedDelay(Duration.ofSeconds(1)))
            .execute((taskInstance, executionContext) -> {
                System.out.println("Ran");
            });

        SchedulerClient client = SchedulerClient.Builder.create(dataSource).build();
        client.schedule(unresolvedTask.instance(RecurringTask.INSTANCE), Instant.now());
        client.schedule(unresolvedTask2.instance(RecurringTask.INSTANCE), Instant.now().plusSeconds(10));

        final Scheduler scheduler = Scheduler
                .create(dataSource)
                .pollingInterval(Duration.ofSeconds(1))
                .heartbeatInterval(Duration.ofSeconds(5))
                .deleteUnresolvedAfter(Duration.ofSeconds(20))
                .build();

        ExampleHelpers.registerShutdownHook(scheduler);

        scheduler.start();

        IntStream.range(0, 5).forEach(i -> {
            scheduler.fetchScheduledExecutions(e -> {});
            scheduler.getFailingExecutions(Duration.ZERO);
        });
    }

}
