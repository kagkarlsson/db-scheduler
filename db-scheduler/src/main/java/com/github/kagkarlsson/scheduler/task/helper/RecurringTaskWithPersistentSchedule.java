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

import com.github.kagkarlsson.scheduler.task.AbstractTask;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.SchedulableTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;

public abstract class RecurringTaskWithPersistentSchedule<T extends ScheduleAndData>
    extends AbstractTask<T> {

  public static final int DEFAULT_PRIORITY = RecurringTask.DEFAULT_PRIORITY;

  public RecurringTaskWithPersistentSchedule(String name, Class<T> dataClass) {
    this(name, dataClass, new FailureHandler.OnFailureRescheduleUsingTaskDataSchedule<>());
  }

  public RecurringTaskWithPersistentSchedule(
      String name, Class<T> dataClass, FailureHandler<T> onFailure) {
    this(name, dataClass, onFailure, DEFAULT_PRIORITY);
  }

  public RecurringTaskWithPersistentSchedule(
      String name, Class<T> dataClass, FailureHandler<T> onFailure, int defaultPriority) {
    super(
        name,
        dataClass,
        onFailure,
        new DeadExecutionHandler.ReviveDeadExecution<>(),
        defaultPriority);
  }

  @Override
  public TaskInstance<T> instance(String id) {
    throw new UnsupportedOperationException(
        "Cannot instatiate a RecurringTaskWithPersistentSchedule without 'data' since that holds the schedule.");
  }

  @Override
  public SchedulableInstance<T> schedulableInstance(String id) {
    throw new UnsupportedOperationException(
        "Cannot instatiate a RecurringTaskWithPersistentSchedule without 'data' since that holds the schedule.");
  }

  @Override
  public SchedulableInstance<T> schedulableInstance(String id, T data) {
    return new SchedulableTaskInstance<>(
        instanceBuilder(id).data(data).build(), data.getSchedule()::getInitialExecutionTime);
  }

  @Override
  public String toString() {
    return RecurringTaskWithPersistentSchedule.class.getName() + " name=" + getName();
  }
}
