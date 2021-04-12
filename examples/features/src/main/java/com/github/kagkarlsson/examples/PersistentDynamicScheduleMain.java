package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static java.util.function.Function.identity;

public class PersistentDynamicScheduleMain extends Example {

    public static void main(String[] args) {
        new PersistentDynamicScheduleMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        final SerializableCronSchedule initialSchedule = new SerializableCronSchedule("*/10 * * * * ?");
        final CustomTask<SerializableCronSchedule> task = Tasks.custom("dynamic-recurring-task", SerializableCronSchedule.class)
            .scheduleOnStartup(RecurringTask.INSTANCE, initialSchedule, initialSchedule)
            .onFailure((executionComplete, executionOperations) -> {
                final SerializableCronSchedule persistedSchedule = (SerializableCronSchedule) (executionComplete.getExecution().taskInstance.getData());
                executionOperations.reschedule(executionComplete, persistedSchedule.getNextExecutionTime(executionComplete));
            })
            .execute((taskInstance, executionContext) -> {
                final SerializableCronSchedule persistentSchedule = taskInstance.getData();
                System.out.println("Ran using persistent schedule: " + persistentSchedule.getCronPattern());

                return (executionComplete, executionOperations) -> {
                    executionOperations.reschedule(
                        executionComplete,
                        persistentSchedule.getNextExecutionTime(executionComplete)
                    );
                };
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource)
            .pollingInterval(Duration.ofSeconds(5))
            .startTasks(task)
            .registerShutdownHook()
            .build();

        scheduler.start();

        sleep(7_000);

        final SerializableCronSchedule newSchedule = new SerializableCronSchedule("*/15 * * * * ?");
        final TaskInstanceId scheduledExecution = TaskInstanceId.of("dynamic-recurring-task", RecurringTask.INSTANCE);
        final Instant newNextExecutionTime = newSchedule.getNextExecutionTime(ExecutionComplete.simulatedSuccess(Instant.now()));

        // reschedule updating both next execution time and the persistent schedule
        System.out.println("Simulating dynamic reschedule of recurring task");
        scheduler.reschedule(scheduledExecution, newNextExecutionTime, newSchedule);
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static class SerializableCronSchedule implements Serializable, Schedule {
        private final String cronPattern;
        SerializableCronSchedule(String cronPattern) {
            this.cronPattern = cronPattern;
        }

        @Override
        public Instant getNextExecutionTime(ExecutionComplete executionComplete) {
            return new CronSchedule(cronPattern).getNextExecutionTime(executionComplete);
        }

        @Override
        public boolean isDeterministic() {
            return true;
        }

        @Override
        public String toString() {
            return "SerializableCronSchedule pattern=" + cronPattern;
        }

        public String getCronPattern() {
            return cronPattern;
        }
    }
}
