/**
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
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;

public class SpawningOtherTasksMain extends Example {

  public static void main(String[] args) {
    new SpawningOtherTasksMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {

    final OneTimeTask<Integer> printer =
        Tasks.oneTime("printer", Integer.class)
            .execute(
                (taskInstance, executionContext) -> {
                  System.out.println("Printer: " + taskInstance.getData());
                });

    final RecurringTask<Void> spawner =
        Tasks.recurring("spawner", FixedDelay.ofSeconds(5))
            .execute(
                (taskInstance, executionContext) -> {
                  final SchedulerClient client = executionContext.getSchedulerClient();
                  final long id = System.currentTimeMillis();

                  System.out.println("Scheduling printer executions.");
                  for (int i = 0; i < 5; i++) {
                    client.schedule(printer.instance("print" + id + i, i), Instant.now());
                  }
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, printer)
            .pollingInterval(Duration.ofSeconds(1))
            .startTasks(spawner)
            .registerShutdownHook()
            .build();

    scheduler.start();
  }
}
