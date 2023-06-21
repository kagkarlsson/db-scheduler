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
import com.github.kagkarlsson.examples.helpers.ExampleHelpers;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class SerialMain extends Example {

  public static void main(String[] args) {
    new SerialMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {

    final CustomTask<Void> task =
        Tasks.custom("task-a", Void.class)
            .execute(
                (taskInstance, executionContext) -> {
                  SchedulerClient client = executionContext.getSchedulerClient();
                  long currentlyRunning =
                      client
                          .getScheduledExecutionsForTask(
                              "task-a",
                              Object.class,
                              ScheduledExecutionsFilter.all().withPicked(true))
                          .size();
                  if (currentlyRunning > 1) {
                    // An execution of the same type is already running. Delay this one.
                    int delaySeconds = 2 + new Random().nextInt(6);
                    System.out.println(
                        "Delaying execution for " + delaySeconds + "s: " + taskInstance);
                    return new RetryLater(Duration.ofSeconds(delaySeconds));
                  }

                  System.out.println("Task a executed");
                  return new CompletionHandler.OnCompleteRemove<>();
                });

    final SchedulerClient client = SchedulerClient.Builder.create(dataSource, task).build();
    final Instant now = Instant.now();
    for (int i = 0; i < 5; i++) {
      client.schedule(task.instance("id" + i), now);
    }

    final Scheduler scheduler =
        Scheduler.create(dataSource, task).pollingInterval(Duration.ofSeconds(1)).build();

    ExampleHelpers.registerShutdownHook(scheduler);

    scheduler.start();
  }

  private class RetryLater implements CompletionHandler<Void> {
    private final Duration delay;

    public RetryLater(Duration delay) {
      this.delay = delay;
    }

    @Override
    public void complete(
        ExecutionComplete executionComplete, ExecutionOperations<Void> executionOperations) {
      executionOperations.reschedule(executionComplete, Instant.now().plus(delay));
    }
  }
}
