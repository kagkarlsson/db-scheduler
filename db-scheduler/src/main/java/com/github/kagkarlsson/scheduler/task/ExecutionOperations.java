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
package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.TaskRepository;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import java.time.Instant;

/**
 * Provides operations that can be performed on a task execution during its lifecycle. This class
 * encapsulates common operations such as stopping, rescheduling, and removing executions.
 *
 * @param <T> the type of the task data
 */
public class ExecutionOperations<T> {

  private final TaskRepository taskRepository;
  private final SchedulerListeners schedulerListeners;
  private final Execution execution;

  public ExecutionOperations(
      TaskRepository taskRepository, SchedulerListeners schedulerListeners, Execution execution) {
    this.taskRepository = taskRepository;
    this.schedulerListeners = schedulerListeners;
    this.execution = execution;
  }

  /**
   * Stops the current execution by removing it from the task repository. This is an alias for
   * {@link #remove()}.
   */
  public void stop() {
    remove();
  }

  /**
   * Removes the current execution from the task repository. After this operation, the execution
   * will no longer be tracked or executed.
   */
  public void remove() {
    taskRepository.remove(execution);
  }

  /**
   * Removes the current execution and schedules a new execution in its place. This is an atomic
   * operation that replaces the current execution with a new one.
   *
   * @param schedulableInstance the new schedulable instance to be scheduled
   */
  public void removeAndScheduleNew(SchedulableInstance<?> schedulableInstance) {
    Instant executionTime = taskRepository.replace(execution, schedulableInstance);
    hintExecutionScheduled(schedulableInstance.getTaskInstance(), executionTime);
  }

  /**
   * Reschedules the current execution to run at a specified time. The consecutive failure count
   * will be incremented. The task data will remain unchanged.
   *
   * @param completed the execution completion information
   * @param nextExecutionTime the time when the execution should run next
   */
  public void reschedule(ExecutionComplete completed, Instant nextExecutionTime) {
    reschedule(completed, nextExecutionTime, null);
  }

  /**
   * Reschedules the current execution to run at a specified time with new data. The consecutive
   * failure count will be incremented.
   *
   * @param completed the execution completion information
   * @param nextExecutionTime the time when the execution should run next
   * @param newData the new data to be associated with the execution
   */
  public void reschedule(ExecutionComplete completed, Instant nextExecutionTime, T newData) {
    rescheduleInternal(completed, nextExecutionTime, newData, execution.consecutiveFailures + 1);
  }

  /**
   * Reschedules the current execution to run at a specified time and resets the consecutive failure
   * count to zero. The task data will remain unchanged.
   *
   * @param completed the execution completion information
   * @param nextExecutionTime the time when the execution should run next
   */
  public void rescheduleAndResetFailures(ExecutionComplete completed, Instant nextExecutionTime) {
    rescheduleInternal(completed, nextExecutionTime, null, 0);
  }

  /**
   * Reschedules the current execution to run at a specified time with new data and resets the
   * consecutive failure count to zero.
   *
   * @param completed the execution completion information
   * @param nextExecutionTime the time when the execution should run next
   * @param newData the new data to be associated with the execution
   */
  public void rescheduleAndResetFailures(
      ExecutionComplete completed, Instant nextExecutionTime, T newData) {
    rescheduleInternal(completed, nextExecutionTime, newData, 0);
  }

  private void rescheduleInternal(
      ExecutionComplete completed, Instant nextExecutionTime, T newData, int consecutiveFailures) {
    if (completed.getResult() == ExecutionComplete.Result.OK) {
      taskRepository.reschedule(
          execution,
          nextExecutionTime,
          newData,
          completed.getTimeDone(),
          execution.lastFailure,
          consecutiveFailures);
    } else {
      taskRepository.reschedule(
          execution,
          nextExecutionTime,
          newData,
          execution.lastSuccess,
          completed.getTimeDone(),
          consecutiveFailures);
    }
    hintExecutionScheduled(completed.getExecution().taskInstance, nextExecutionTime);
  }

  private void hintExecutionScheduled(TaskInstanceId taskInstanceId, Instant nextExecutionTime) {
    // Hint that a new execution was scheduled in-case we want to go check for it immediately
    schedulerListeners.onExecutionScheduled(taskInstanceId, nextExecutionTime);
  }
}
