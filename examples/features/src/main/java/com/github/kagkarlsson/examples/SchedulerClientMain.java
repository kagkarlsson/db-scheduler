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
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Instant;
import javax.sql.DataSource;

public class SchedulerClientMain extends Example {

  public static void main(String[] args) {
    new SchedulerClientMain().runWithDatasource();
  }

  public static final TaskDescriptor<Integer> MY_TASK = TaskDescriptor.of("task-a", Integer.class);

  @Override
  public void run(DataSource dataSource) {

    final OneTimeTask<Integer> task =
        Tasks.oneTime(MY_TASK)
            .execute(
                (taskInstance, executionContext) -> {
                  System.out.println("Task a executed");
                });

    final SchedulerClient clientWithTypeInformation =
        SchedulerClient.Builder.create(dataSource, task)
            .serializer(new JacksonSerializer())
            .build();

    final Instant now = Instant.now();
    for (int i = 0; i < 5; i++) {
      clientWithTypeInformation.scheduleIfNotExists(
          MY_TASK.instance("id" + i).data(i).scheduledTo(now.plusSeconds(i)));
    }

    System.out.println("Listing scheduled executions");
    clientWithTypeInformation
        .getScheduledExecutions(ScheduledExecutionsFilter.all())
        .forEach(
            execution -> {
              System.out.printf(
                  "Scheduled execution: taskName=%s, instance=%s, executionTime=%s, data=%s%n",
                  execution.getTaskInstance().getTaskName(),
                  execution.getTaskInstance().getId(),
                  execution.getExecutionTime(),
                  execution.getData());
            });

    final SchedulerClient rawClient = SchedulerClient.Builder.create(dataSource).build();
    System.out.println(
        "Listing scheduled executions for client with no known tasks (data-classes and implementations)");
    rawClient
        .getScheduledExecutions(ScheduledExecutionsFilter.all())
        .forEach(
            execution -> {
              System.out.printf(
                  "Scheduled execution: taskName=%s, instance=%s, executionTime=%s, data=%s%n",
                  execution.getTaskInstance().getTaskName(),
                  execution.getTaskInstance().getId(),
                  execution.getExecutionTime(),
                  new String((byte[]) execution.getData()));
            });
  }
}
