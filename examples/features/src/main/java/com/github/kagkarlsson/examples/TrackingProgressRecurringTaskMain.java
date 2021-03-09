package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Duration;

import static java.util.function.Function.identity;

public class TrackingProgressRecurringTaskMain extends Example {

    public static void main(String[] args) {
        new TrackingProgressRecurringTaskMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        final FixedDelay schedule = FixedDelay.ofSeconds(2);

        // In the future RecurringTaskBuilder will be adapted to fit this particular use-case better
        // but for now, custom-task builder must be used
        final CustomTask<Counter> statefulTask = Tasks.custom("counting-task", Counter.class)
            .scheduleOnStartup(RecurringTask.INSTANCE, new Counter(0), identity())
            .onFailureReschedule(schedule)
            .execute((taskInstance, executionContext) -> {
                final Counter startingCounter = taskInstance.getData();
                for (int i = 0; i < 10; i++) {
                    System.out.println("Counting " + (startingCounter.value + i));
                }
                return (executionComplete, executionOperations) -> {
                    executionOperations.reschedule(
                        executionComplete,
                        schedule.getNextExecutionTime(executionComplete),
                        new Counter(startingCounter.value + 10));    // new value to be persisted as task_data for the next run
                };
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource)
            .pollingInterval(Duration.ofSeconds(5))
            .startTasks(statefulTask)
            .registerShutdownHook()
            .build();

        scheduler.start();
    }

    private static final class Counter implements Serializable {
        private final int value;

        public Counter(int value) {
            this.value = value;
        }
    }
}
