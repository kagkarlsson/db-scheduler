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
package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.examples.boot.ExampleContext;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskWithDataDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Instant;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import utils.EventLogger;

@Configuration
public class RecurringStateTrackingConfiguration {

  public static final TaskWithDataDescriptor<Integer> STATE_TRACKING_RECURRING_TASK =
      new TaskWithDataDescriptor<>("state-tracking-recurring-task", Integer.class);

  /** Start the example */
  public static void start(ExampleContext ctx) {
    Integer data = 1;
    ctx.log(
        "Starting recurring task "
            + STATE_TRACKING_RECURRING_TASK.getTaskName()
            + " with initial data: "
            + data
            + ". Initial execution-time will be now (deviating from defined schedule).");

    ctx.schedulerClient.schedule(
        STATE_TRACKING_RECURRING_TASK.instance(RecurringTask.INSTANCE, data),
        Instant.now() // start-time, will run according to schedule after this
        );
  }

  /** Bean definition */
  @Bean
  public Task<Integer> stateTrackingRecurring() {
    return Tasks.recurring(STATE_TRACKING_RECURRING_TASK, Schedules.cron("0/5 * * * * *"))
        .doNotScheduleOnStartup() // just for demo-purposes, so we can start it on-demand
        .executeStateful(
            (TaskInstance<Integer> taskInstance, ExecutionContext executionContext) -> {
              EventLogger.logTask(
                  STATE_TRACKING_RECURRING_TASK,
                  "Ran recurring task. Will keep running according to the same schedule, "
                      + "but the state is updated. State: "
                      + taskInstance.getData());

              // Stateful recurring return the updated state as the final step (convenience)
              return taskInstance.getData() + 1;
            });
  }
}
