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

import com.github.kagkarlsson.scheduler.jdbc.DeactivationUpdate;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import java.time.Instant;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface CompletionHandler<T> {

  void complete(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations);

  class OnCompleteRemove<T> implements CompletionHandler<T> {

    @Override
    public void complete(
        ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
      executionOperations.remove();
    }
  }

  class OnCompleteDeactivate<T> implements CompletionHandler<T> {
    private State state;

    public OnCompleteDeactivate(State state) {
      this.state = state;
    }

    @Override
    public void complete(
        ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
      executionOperations.deactivate(DeactivationUpdate.toState(state).build());
    }
  }

  class OnCompleteReschedule<T> implements CompletionHandler<T> {

    private static final Logger LOG = LoggerFactory.getLogger(OnCompleteReschedule.class);
    private final Schedule schedule;
    private final boolean setNewData;
    private T newData;

    public OnCompleteReschedule(Schedule schedule) {
      this.schedule = schedule;
      this.setNewData = false;
    }

    public OnCompleteReschedule(Schedule schedule, T newData) {
      this.schedule = schedule;
      this.newData = newData;
      this.setNewData = true;
    }

    @Override
    public void complete(
        ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
      Instant nextExecution = schedule.getNextExecutionTime(executionComplete);
      LOG.debug(
          "Rescheduling task {} to {}",
          executionComplete.getExecution().taskInstance,
          nextExecution);
      if (setNewData) {
        executionOperations.reschedule(executionComplete, nextExecution, newData);
      } else {
        executionOperations.reschedule(executionComplete, nextExecution);
      }
    }

    @Override
    public String toString() {
      return "OnCompleteReschedule with " + schedule;
    }
  }

  class OnCompleteReplace<CURRENT_TYPE, NEXT_TYPE> implements CompletionHandler<CURRENT_TYPE> {

    private static final Logger LOG = LoggerFactory.getLogger(OnCompleteReplace.class);
    private final Function<TaskInstance<CURRENT_TYPE>, SchedulableInstance<NEXT_TYPE>>
        newInstanceCreator;
    private String newTaskName = "<hidden>"; // used for logging purposes only

    public OnCompleteReplace(String newTaskName) {
      this(newTaskName, null);
    }

    public OnCompleteReplace(String newTaskName, NEXT_TYPE newData) {
      this(
          (TaskInstance<CURRENT_TYPE> currentInstance) -> {
            return SchedulableInstance.of(
                new TaskInstance<>(newTaskName, currentInstance.getId(), newData), Instant.now());
          });
      this.newTaskName = newTaskName;
    }

    public OnCompleteReplace(TaskDescriptor<NEXT_TYPE> newTask, NEXT_TYPE newData) {
      this(
          (TaskInstance<CURRENT_TYPE> currentInstance) ->
              newTask.instance(currentInstance.getId()).data(newData).scheduledTo(Instant.now()));
      this.newTaskName = newTask.getTaskName();
    }

    public OnCompleteReplace(
        Function<TaskInstance<CURRENT_TYPE>, SchedulableInstance<NEXT_TYPE>> newInstanceCreator) {
      this.newInstanceCreator = newInstanceCreator;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void complete(
        ExecutionComplete executionComplete,
        ExecutionOperations<CURRENT_TYPE> executionOperations) {
      TaskInstance<CURRENT_TYPE> currentInstance = executionComplete.getExecution().taskInstance;
      SchedulableInstance<NEXT_TYPE> nextInstance = newInstanceCreator.apply(currentInstance);
      LOG.debug(
          "Removing instance {} and scheduling instance {}",
          executionComplete.getExecution().taskInstance,
          nextInstance);
      executionOperations.removeAndScheduleNew(nextInstance);
    }

    @Override
    public String toString() {
      return "OnCompleteReplace with task '" + newTaskName + "'";
    }
  }
}
