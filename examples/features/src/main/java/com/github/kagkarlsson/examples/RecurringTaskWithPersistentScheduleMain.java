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
import com.github.kagkarlsson.scheduler.task.helper.RecurringTaskWithPersistentSchedule;
import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.*;
import java.time.Duration;
import javax.sql.DataSource;

public class RecurringTaskWithPersistentScheduleMain extends Example {

  public static void main(String[] args) {
    new RecurringTaskWithPersistentScheduleMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {

    final RecurringTaskWithPersistentSchedule<ScheduleAndNoData> task =
        Tasks.recurringWithPersistentSchedule("dynamic-recurring-task", ScheduleAndNoData.class)
            .execute(
                (taskInstance, executionContext) -> {
                  System.out.println(
                      "Instance: '"
                          + taskInstance.getId()
                          + "' ran using persistent schedule: "
                          + taskInstance.getData().getSchedule());
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, task)
            .pollingInterval(Duration.ofSeconds(1))
            .registerShutdownHook()
            .build();

    scheduler.start();
    sleep(2_000);

    scheduler.schedule(
        task.schedulableInstance(
            "id1", new ScheduleAndNoData(Schedules.fixedDelay(Duration.ofSeconds(1)))));
    scheduler.schedule(
        task.schedulableInstance(
            "id2", new ScheduleAndNoData(Schedules.fixedDelay(Duration.ofSeconds(6)))));
  }

  public static class ScheduleAndNoData implements ScheduleAndData {
    private static final long serialVersionUID = 1L; // recommended when using Java serialization
    private final FixedDelay schedule;

    private ScheduleAndNoData() {
      this(null);
    }

    public ScheduleAndNoData(FixedDelay schedule) {
      this.schedule = schedule;
    }

    @Override
    public FixedDelay getSchedule() {
      return schedule;
    }

    @Override
    public Object getData() {
      return null;
    }
  }
}
