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

import static java.time.Duration.*;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Instant;
import javax.sql.DataSource;

public class ExponentialBackoffMain extends Example {

  public static void main(String[] args) {
    new ExponentialBackoffMain().runWithDatasource();
  }

  public static final TaskDescriptor<Void> MY_TASK =
      TaskDescriptor.of("exponential_backoff_task", Void.class);

  @Override
  public void run(DataSource dataSource) {

    OneTimeTask<Void> failingTask =
        Tasks.oneTime(MY_TASK)
            .onFailure(new FailureHandler.ExponentialBackoffFailureHandler<>(ofSeconds(1)))
            .execute(
                (taskInstance, executionContext) -> {
                  throw new RuntimeException("simulated task exception");
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, failingTask)
            .pollingInterval(ofSeconds(2))
            .registerShutdownHook()
            .build();

    scheduler.schedule(MY_TASK.instance("1").scheduledTo(Instant.now()));

    scheduler.start();
  }
}
