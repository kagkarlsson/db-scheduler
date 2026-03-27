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
package com.github.kagkarlsson.examples.boot.config;

import static com.github.kagkarlsson.scheduler.task.schedule.Schedules.fixedDelay;

import com.github.kagkarlsson.examples.boot.CounterService;
import com.github.kagkarlsson.examples.boot.ExampleContext;
import com.github.kagkarlsson.scheduler.boot.config.RecurringTask;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import utils.EventLogger;

@Configuration
public class BasicExamplesConfiguration {

  public static final TaskDescriptor<Void> BASIC_ONE_TIME_TASK =
      TaskDescriptor.of("sample-one-time-task");
  public static final TaskDescriptor<Void> BASIC_RECURRING_TASK =
      TaskDescriptor.of("recurring-sample-task");
  private static final Logger log = LoggerFactory.getLogger(BasicExamplesConfiguration.class);
  private static int ID = 1;

  private final CounterService counter;

  public BasicExamplesConfiguration(CounterService counter) {
    this.counter = counter;
  }

  /** Start the example */
  public static void triggerOneTime(ExampleContext ctx) {
    ctx.log(
        "Scheduling a basic one-time task to run 'Instant.now()+seconds'. If seconds=0, the scheduler will pick "
            + "these up immediately since it is configured with 'immediate-execution-enabled=true'");

    ctx.schedulerClient.scheduleIfNotExists(
        BASIC_ONE_TIME_TASK.instance(String.valueOf(ID++)).scheduledTo(Instant.now()));
  }

  /**
   * Define a recurring task with a dependency, which will automatically be picked up by the Spring
   * Boot autoconfiguration.
   */
  @Bean
  Task<Void> recurringSampleTask(CounterService counter) {
    return Tasks.recurring(BASIC_RECURRING_TASK, fixedDelay(Duration.ofMinutes(1)))
        .execute(
            (instance, ctx) -> {
              log.info("Running recurring-simple-task. Instance: {}, ctx: {}", instance, ctx);
              counter.increase();
              EventLogger.logTask(
                  BASIC_RECURRING_TASK, "Ran. Run-counter current-value=" + counter.read());
            });
  }

  /** A recurring task with dependencies from the current class using annotation. */
  @RecurringTask(name = "recurring-sample-task-annotation", cron = "*/30 * * * * *")
  public void recurringSampleTaskAnnotation(TaskInstance<Void> instance, ExecutionContext ctx) {
    log.info("Running recurring-sample-task-annotation. Instance: {}, ctx: {}", instance, ctx);
    counter.increase();
    EventLogger.logTask(
        "recurring-sample-task-annotation", "Ran. Run-counter current-value=" + counter.read());
  }

  /** Define a recurring task with no dependencies and no inputs using annotation. */
  @RecurringTask(
      name = "recurring-sample-task-annotation-no-inputs",
      cron = "${recurring-sample-task-annotation-no-inputs.cron}")
  public void recurringSampleTaskAnnotationNoInputs() {
    log.info("Running recurring-sample-task-annotation-no-inputs.");
  }

  /** Define a one-time task which have to be manually scheduled. */
  @Bean
  Task<Void> sampleOneTimeTask() {
    return Tasks.oneTime(BASIC_ONE_TIME_TASK)
        .execute(
            (instance, ctx) -> {
              log.info("I am a one-time task!");
            });
  }
}
