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
package com.github.kagkarlsson.scheduler.task.helper;

import com.github.kagkarlsson.scheduler.Clock;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.CompletionHandler.OnCompleteReschedule;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler.ReviveDeadExecution;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;

public abstract class RecurringTask<T> extends AbstractTask<T> implements OnStartup {

  public static final String INSTANCE = "recurring";
  private final OnCompleteReschedule<T> onComplete;
  private final Schedule schedule;
  private final ScheduleOnStartup<T> scheduleOnStartup;

  public RecurringTask(String name, Schedule schedule, Class<T> dataClass) {
    this(
        name,
        schedule,
        dataClass,
        new ScheduleRecurringOnStartup<>(INSTANCE, null, schedule),
        new FailureHandler.OnFailureReschedule<T>(schedule),
        new ReviveDeadExecution<>());
  }

  public RecurringTask(
      String name, Schedule schedule, Class<T> dataClass, FailureHandler<T> failureHandler) {
    this(
        name,
        schedule,
        dataClass,
        new ScheduleRecurringOnStartup<>(INSTANCE, null, schedule),
        failureHandler,
        new ReviveDeadExecution<>());
  }

  public RecurringTask(String name, Schedule schedule, Class<T> dataClass, T initialData) {
    this(
        name,
        schedule,
        dataClass,
        new ScheduleRecurringOnStartup<>(INSTANCE, initialData, schedule),
        new FailureHandler.OnFailureReschedule<T>(schedule),
        new ReviveDeadExecution<>());
  }

  public RecurringTask(
      String name,
      Schedule schedule,
      Class<T> dataClass,
      ScheduleRecurringOnStartup<T> scheduleOnStartup,
      FailureHandler<T> failureHandler,
      DeadExecutionHandler<T> deadExecutionHandler) {
    super(name, dataClass, failureHandler, deadExecutionHandler);
    onComplete = new OnCompleteReschedule<>(schedule);
    this.schedule = schedule;
    this.scheduleOnStartup = scheduleOnStartup;
  }

  @Override
  public int getDefaultPriority() {
    return Priority.HIGH;
  }

  @Override
  public SchedulableInstance<T> schedulableInstance(String id) {
    return new SchedulableTaskInstance<>(
        new TaskInstance<>(getName(), id), schedule::getInitialExecutionTime);
  }

  @Override
  public SchedulableInstance<T> schedulableInstance(String id, T data) {
    return new SchedulableTaskInstance<>(
        new TaskInstance<>(getName(), id, data), schedule::getInitialExecutionTime);
  }

  @Override
  public void onStartup(SchedulerClient scheduler, Clock clock) {
    if (scheduleOnStartup != null) {
      scheduleOnStartup.apply(scheduler, clock, this);
    }
  }

  @Override
  public CompletionHandler<T> execute(
      TaskInstance<T> taskInstance, ExecutionContext executionContext) {
    executeRecurringly(taskInstance, executionContext);
    return onComplete;
  }

  public abstract void executeRecurringly(
      TaskInstance<T> taskInstance, ExecutionContext executionContext);

  public TaskInstanceId getDefaultTaskInstance() {
    return TaskInstanceId.of(name, INSTANCE);
  }

  @Override
  public String toString() {
    return "RecurringTask name=" + getName() + ", onComplete=" + onComplete;
  }
}
