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

import static com.github.kagkarlsson.examples.helpers.ExampleHelpers.sleep;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.examples.helpers.ExampleHelpers;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;

public class SchedulerMain extends Example {

  public static void main(String[] args) {
    new SchedulerMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {

    // recurring with no data
    RecurringTask<Void> recurring1 =
        Tasks.recurring("recurring_no_data", FixedDelay.of(Duration.ofSeconds(5)))
            .onFailureReschedule() // default
            .onDeadExecutionRevive() // default
            .execute(
                (taskInstance, executionContext) -> {
                  sleep(100);
                  System.out.println("Executing " + taskInstance.getTaskAndInstance());
                });

    // recurring with contant data
    RecurringTask<Integer> recurring2 =
        Tasks.recurring(
                "recurring_constant_data", FixedDelay.of(Duration.ofSeconds(7)), Integer.class)
            .initialData(1)
            .onFailureReschedule() // default
            .onDeadExecutionRevive() // default
            .execute(
                (taskInstance, executionContext) -> {
                  sleep(100);
                  System.out.println(
                      "Executing "
                          + taskInstance.getTaskAndInstance()
                          + " , data: "
                          + taskInstance.getData());
                });

    // recurring with changing data
    Schedule custom1Schedule = FixedDelay.of(Duration.ofSeconds(4));
    CustomTask<Integer> custom1 =
        Tasks.custom("recurring_changing_data", Integer.class)
            .scheduleOnStartup("instance1", 1, custom1Schedule::getInitialExecutionTime)
            .onFailureReschedule(custom1Schedule) // default
            .onDeadExecutionRevive() // default
            .execute(
                (taskInstance, executionContext) -> {
                  System.out.println(
                      "Executing "
                          + taskInstance.getTaskAndInstance()
                          + " , data: "
                          + taskInstance.getData());
                  return (executionComplete, executionOperations) -> {
                    sleep(100);
                    Instant nextExecutionTime =
                        custom1Schedule.getNextExecutionTime(executionComplete);
                    int newData = taskInstance.getData() + 1;
                    executionOperations.reschedule(executionComplete, nextExecutionTime, newData);
                  };
                });

    // one-time with no data
    OneTimeTask<Void> onetime1 =
        Tasks.oneTime("onetime_no_data")
            .onDeadExecutionRevive() // default
            .onFailureRetryLater() // default
            .execute(
                (TaskInstance<Void> taskInstance, ExecutionContext executionContext) -> {
                  sleep(100);
                  System.out.println("Executing " + taskInstance.getTaskAndInstance());
                });

    // one-time with data
    OneTimeTask<Integer> onetime2 =
        Tasks.oneTime("onetime_withdata", Integer.class)
            .onFailureRetryLater() // default
            .execute(
                (TaskInstance<Integer> taskInstance, ExecutionContext executionContext) -> {
                  sleep(100);
                  System.out.println(
                      "Executing "
                          + taskInstance.getTaskAndInstance()
                          + " , data: "
                          + taskInstance.getData());
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, onetime1, onetime2)
            .startTasks(recurring1, recurring2, custom1)
            .build();

    ExampleHelpers.registerShutdownHook(scheduler);

    scheduler.start();

    sleep(3000);

    scheduler.schedule(onetime1.instance("onetime1_directly"), Instant.now());
    scheduler.schedule(onetime2.instance("onetime2", 100), Instant.now().plusSeconds(3));

    scheduler.schedule(onetime2.instance("onetime3", 100), Instant.now());
  }
}
