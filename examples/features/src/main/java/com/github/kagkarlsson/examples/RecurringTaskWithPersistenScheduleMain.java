package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTaskWithPersistentSchedule;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Duration;

public class RecurringTaskWithPersistenScheduleMain extends Example {

    public static void main(String[] args) {
        new RecurringTaskWithPersistenScheduleMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        final RecurringTaskWithPersistentSchedule<SerializableCronSchedule> task =
            Tasks.recurringWithPersistentSchedule("dynamic-recurring-task", SerializableCronSchedule.class)
                .execute((taskInstance, executionContext) -> {
                    System.out.println("Instance: '" + taskInstance.getId() + "' ran using persistent schedule: " + taskInstance.getData().getSchedule());
                });

        final Scheduler scheduler = Scheduler
            .create(dataSource, task)
            .pollingInterval(Duration.ofSeconds(1))
            .registerShutdownHook()
            .build();

        scheduler.start();
        sleep(2_000);

        scheduler.schedule(task.schedulableInstance("id1", new SerializableCronSchedule("0/6 * * * * ?")));
        scheduler.schedule(task.schedulableInstance("id2", new SerializableCronSchedule("3/6 * * * * ?")));
    }

    private static class SerializableCronSchedule implements Serializable, Tasks.ScheduleAndData {
        private final String cronPattern;

        SerializableCronSchedule(String cronPattern) {
            this.cronPattern = cronPattern;
        }

        @Override
        public String toString() {
            return "SerializableCronSchedule pattern=" + cronPattern;
        }

        @Override
        public Schedule getSchedule() {
            return new CronSchedule(cronPattern); // FIXLATER: possibly cache this, probably not necessary
        }

        @Override
        public Object getData() {
            return null; // null for now
        }
    }
}
