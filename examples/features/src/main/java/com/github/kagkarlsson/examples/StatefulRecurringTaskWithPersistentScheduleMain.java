/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.PlainScheduleAndData;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTaskWithPersistentSchedule;
import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;

import javax.sql.DataSource;
import java.time.Duration;

public class StatefulRecurringTaskWithPersistentScheduleMain extends Example {

    public static void main(String[] args) {
        new StatefulRecurringTaskWithPersistentScheduleMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        final RecurringTaskWithPersistentSchedule<ScheduleAndInteger> task =
            Tasks.recurringWithPersistentSchedule("dynamic-recurring-task", ScheduleAndInteger.class)
                .executeStateful((taskInstance, executionContext) -> {
                    System.out.printf("Instance: '%s' ran using persistent schedule '%s' and data '%s'\n", taskInstance.getId(), taskInstance.getData().getSchedule(), taskInstance.getData().getData());
                    return taskInstance.getData().returnIncremented();
                });

        final Scheduler scheduler = Scheduler
            .create(dataSource, task)
            .pollingInterval(Duration.ofSeconds(1))
            .registerShutdownHook()
            .build();

        scheduler.start();
        sleep(2_000);

        scheduler.schedule(task.schedulableInstance("id1",
            new ScheduleAndInteger(
                Schedules.fixedDelay(Duration.ofSeconds(3)),
                1)));
    }

    public static class ScheduleAndInteger implements ScheduleAndData {
        private final Schedule schedule;
        private final Integer data;

        public ScheduleAndInteger() {
            this(null, null);
        }

        public ScheduleAndInteger(Schedule schedule, Integer data) {
            this.schedule = schedule;
            this.data = data;
        }

        @Override
        public Schedule getSchedule() {
            return schedule;
        }

        @Override
        public Integer getData() {
            return data;
        }

        public ScheduleAndInteger returnIncremented() {
            return new ScheduleAndInteger(getSchedule(), getData() + 1);
        }
    }

}
