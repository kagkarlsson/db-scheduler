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
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;

public class EnableImmediateExecutionMain extends Example {

  public static void main(String[] args) {
    new EnableImmediateExecutionMain().runWithDatasource();
  }

  public static final TaskDescriptor<Void> DESCRIPTOR = TaskDescriptor.of("my_task");

  @Override
  public void run(DataSource dataSource) {

    OneTimeTask<Void> onetimeTask =
        Tasks.oneTime(DESCRIPTOR)
            .execute(
                (taskInstance, executionContext) -> {
                  System.out.println("Executed!");
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, onetimeTask)
            .pollingInterval(Duration.ofSeconds(20))
            .enableImmediateExecution()
            .registerShutdownHook()
            .build();

    scheduler.start();

    sleep(2000);
    System.out.println("Scheduling task to executed immediately.");
    scheduler.schedule(DESCRIPTOR.instance("1").scheduledTo(Instant.now()));

    // scheduler.triggerCheckForDueExecutions();
    // another option for triggering execution directly

  }
}
