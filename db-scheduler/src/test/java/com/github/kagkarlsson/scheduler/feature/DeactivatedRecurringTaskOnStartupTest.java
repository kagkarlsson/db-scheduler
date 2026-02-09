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
package com.github.kagkarlsson.scheduler.feature;

import static java.util.Collections.singletonList;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.SchedulerTester;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class DeactivatedRecurringTaskOnStartupTest {
  private final SettableClock clock = new SettableClock();
  private static final TaskDescriptor<Void> TASK = TaskDescriptor.of("recurring-task");
  private static final TaskInstanceId INSTANCE = TASK.instanceId(RecurringTask.INSTANCE);

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  private ManualScheduler scheduler;
  private SchedulerTester tester;

  @BeforeEach
  public void setUp() {
    RecurringTask<Void> task = Tasks.recurring(TASK, Schedules.fixedDelay(Duration.ofHours(1)))
      .execute((instance, ctx) -> {
      });

    scheduler =
      TestHelper.createManualScheduler(postgres.getDataSource())
        .clock(clock)
        .startTasks(singletonList(task))
        .build();

    tester = new SchedulerTester(scheduler);
  }

  @Test
  public void should_not_reactivate_paused_recurring_task_on_restart() {
    scheduler.runOnStartup();
    tester.assertThatExecution(INSTANCE).hasState(State.ACTIVE);

    scheduler.deactivate(INSTANCE, State.PAUSED);
    tester.assertThatExecution(INSTANCE).hasState(State.PAUSED);

    // Run on-startup when execution in PAUSED state
    scheduler.runOnStartup();
    tester.assertThatExecution(INSTANCE).hasState(State.PAUSED);
  }

  @Test
  public void should_remove_paused_recurring_task_when_schedule_is_disabled() {
    scheduler.runOnStartup();
    tester.assertThatExecution(INSTANCE).hasState(State.ACTIVE);

    scheduler.deactivate(INSTANCE, State.PAUSED);
    tester.assertThatExecution(INSTANCE).hasState(State.PAUSED);

    // Create a new scheduler with a disabled schedule ("-" pattern)
    RecurringTask<Void> disabledTask = Tasks.recurring(TASK, Schedules.cron("-"))
      .execute((instance, ctx) -> {});

    ManualScheduler schedulerWithDisabledTask =
      TestHelper.createManualScheduler(postgres.getDataSource())
        .clock(clock)
        .startTasks(singletonList(disabledTask))
        .build();

    // Run on-startup: disabled schedule should remove the paused execution
    schedulerWithDisabledTask.runOnStartup();

    SchedulerTester disabledTester = new SchedulerTester(schedulerWithDisabledTask);
    disabledTester.assertThatExecution(INSTANCE).isRemoved();
  }

}
