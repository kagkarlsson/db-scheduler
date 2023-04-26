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
import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Instant;
import java.util.Random;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import utils.EventLogger;
import utils.Utils;

@Configuration
public class ParallellJobConfiguration {

  public static final TaskWithoutDataDescriptor PARALLEL_JOB_SPAWNER =
      new TaskWithoutDataDescriptor("parallel-job-spawner");
  public static final TaskWithDataDescriptor<Integer> PARALLEL_JOB =
      new TaskWithDataDescriptor<>("parallel-job", Integer.class);
  private TransactionTemplate tx;

  public ParallellJobConfiguration(TransactionTemplate tx) {
    this.tx = tx;
  }

  /** Start the example */
  public static void start(ExampleContext ctx) {
    ctx.log(
        "Starting recurring task "
            + PARALLEL_JOB_SPAWNER.getTaskName()
            + ". Initial execution-time will be now (deviating from defined schedule).");

    ctx.schedulerClient.reschedule(
        PARALLEL_JOB_SPAWNER.instanceId(RecurringTask.INSTANCE), Instant.now());
  }

  /** Bean definition */
  @Bean
  public Task<Void> parallelJobSpawner() {
    return Tasks.recurring(PARALLEL_JOB_SPAWNER, Schedules.cron("0/20 * * * * *"))
        .doNotScheduleOnStartup() // just for demo-purposes, so we can start it on-demand
        .execute(
            (TaskInstance<Void> taskInstance, ExecutionContext executionContext) -> {

              // Create all or none. SchedulerClient is transactions-aware since a Spring datasource
              // is used
              tx.executeWithoutResult(
                  (TransactionStatus status) -> {
                    for (int quarter = 1; quarter < 5; quarter++) {
                      // can use 'executionContext.getSchedulerClient()' to avoid circular
                      // dependency
                      executionContext
                          .getSchedulerClient()
                          .schedule(PARALLEL_JOB.instance("q" + quarter, quarter), Instant.now());
                    }
                  });
              EventLogger.logTask(
                  PARALLEL_JOB_SPAWNER, "Ran. Scheduled tasks for generating quarterly report.");
            });
  }

  @Bean
  public Task<Integer> parallelJob() {
    return Tasks.oneTime(PARALLEL_JOB)
        .execute(
            (TaskInstance<Integer> taskInstance, ExecutionContext executionContext) -> {
              long startTime = System.currentTimeMillis();

              Utils.sleep(new Random().nextInt(10) * 1000);

              String threadName = Thread.currentThread().getName();
              EventLogger.logTask(
                  PARALLEL_JOB,
                  String.format(
                      "Ran. Generated report for quarter Q%s  (in thread '%s', duration %sms)",
                      taskInstance.getData(), threadName, System.currentTimeMillis() - startTime));
            });
  }
}
