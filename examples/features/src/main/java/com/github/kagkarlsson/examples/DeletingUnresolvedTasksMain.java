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
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.IntStream;
import javax.sql.DataSource;

public class DeletingUnresolvedTasksMain extends Example {

  public static final TaskDescriptor<Void> UNRESOLVED_TASK_1 = TaskDescriptor.of("unresolved1");
  public static final TaskDescriptor<Void> UNRESOLVED_TASK_2 = TaskDescriptor.of("unresolved2");

  public static void main(String[] args) {
    new DeletingUnresolvedTasksMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {
    RecurringTask<Void> unresolvedTask =
        Tasks.recurring(UNRESOLVED_TASK_1, Schedules.fixedDelay(Duration.ofSeconds(1)))
            .execute(
                (taskInstance, executionContext) -> {
                  System.out.println("Ran");
                });
    RecurringTask<Void> unresolvedTask2 =
        Tasks.recurring(UNRESOLVED_TASK_2, Schedules.fixedDelay(Duration.ofSeconds(1)))
            .execute(
                (taskInstance, executionContext) -> {
                  System.out.println("Ran");
                });

    SchedulerClient client = SchedulerClient.Builder.create(dataSource).build();
    client.scheduleIfNotExists(
        UNRESOLVED_TASK_1.instance(RecurringTask.INSTANCE).scheduledTo(Instant.now()));
    client.scheduleIfNotExists(
        UNRESOLVED_TASK_2
            .instance(RecurringTask.INSTANCE)
            .scheduledTo(Instant.now().plusSeconds(10)));

    final Scheduler scheduler =
        Scheduler.create(dataSource)
            .pollingInterval(Duration.ofSeconds(1))
            .heartbeatInterval(Duration.ofSeconds(5))
            .deleteUnresolvedAfter(Duration.ofSeconds(20))
            .registerShutdownHook()
            .build();

    scheduler.start();

    IntStream.range(0, 5)
        .forEach(
            i -> {
              scheduler.fetchScheduledExecutions(e -> {});
              scheduler.getFailingExecutions(Duration.ZERO);
            });
  }
}
