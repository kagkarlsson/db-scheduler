/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTaskWithPersistentSchedule;
import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Duration;
import javax.sql.DataSource;

public class StatefulRecurringTaskWithPersistentScheduleMain extends Example {

  public static void main(String[] args) {
    new StatefulRecurringTaskWithPersistentScheduleMain().runWithDatasource();
  }

  public static final TaskDescriptor<ScheduleAndInteger> DYNAMIC_RECURRING_TASK =
      TaskDescriptor.of("dynamic-recurring-task", ScheduleAndInteger.class);

  @Override
  public void run(DataSource dataSource) {

    final RecurringTaskWithPersistentSchedule<ScheduleAndInteger> task =
        Tasks.recurringWithPersistentSchedule(DYNAMIC_RECURRING_TASK)
            .executeStateful(
                (taskInstance, executionContext) -> {
                  System.out.printf(
                      "Instance: '%s' ran using persistent schedule '%s' and data '%s'\n",
                      taskInstance.getId(),
                      taskInstance.getData().getSchedule(),
                      taskInstance.getData().getData());
                  return taskInstance.getData().returnIncremented();
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, task)
            .pollingInterval(Duration.ofSeconds(1))
            .registerShutdownHook()
            .build();

    scheduler.start();
    sleep(2_000);

    scheduler.schedule(
        DYNAMIC_RECURRING_TASK
            .instance("id1")
            .data(new ScheduleAndInteger(Schedules.fixedDelay(Duration.ofSeconds(3)), 1))
            .scheduledAccordingToData());
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
