/**
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.ClientEvent;
import com.github.kagkarlsson.scheduler.ClientEvent.ClientEventContext;
import com.github.kagkarlsson.scheduler.ClientEvent.EventType;
import com.github.kagkarlsson.scheduler.SchedulerClientEventListener;
import com.github.kagkarlsson.scheduler.TaskRepository;
import java.time.Instant;

public class ExecutionOperations<T> {

    private final TaskRepository taskRepository;
    private final SchedulerClientEventListener earlyExecutionListener;
    private final Execution execution;

    public ExecutionOperations(
            TaskRepository taskRepository, SchedulerClientEventListener earlyExecutionListener, Execution execution) {
        this.taskRepository = taskRepository;
        this.earlyExecutionListener = earlyExecutionListener;
        this.execution = execution;
    }

    public void stop() {
        remove();
    }

    public void remove() {
        taskRepository.remove(execution);
    }

    public void removeAndScheduleNew(SchedulableInstance<T> schedulableInstance) {
        Instant executionTime = taskRepository.replace(execution, schedulableInstance);
        hintExecutionScheduled(schedulableInstance.getTaskInstance(), executionTime);
    }

    public void reschedule(ExecutionComplete completed, Instant nextExecutionTime) {
        if (completed.getResult() == ExecutionComplete.Result.OK) {
            taskRepository.reschedule(execution, nextExecutionTime, completed.getTimeDone(), execution.lastFailure, 0);
        } else {
            taskRepository.reschedule(
                    execution,
                    nextExecutionTime,
                    execution.lastSuccess,
                    completed.getTimeDone(),
                    execution.consecutiveFailures + 1);
        }
        hintExecutionScheduled(completed.getExecution().taskInstance, nextExecutionTime);
    }

    public void reschedule(ExecutionComplete completed, Instant nextExecutionTime, T newData) {
        if (completed.getResult() == ExecutionComplete.Result.OK) {
            taskRepository.reschedule(
                    execution, nextExecutionTime, newData, completed.getTimeDone(), execution.lastFailure, 0);
        } else {
            taskRepository.reschedule(
                    execution,
                    nextExecutionTime,
                    newData,
                    execution.lastSuccess,
                    completed.getTimeDone(),
                    execution.consecutiveFailures + 1);
        }
        hintExecutionScheduled(completed.getExecution().taskInstance, nextExecutionTime);
    }

    private void hintExecutionScheduled(TaskInstanceId taskInstanceId, Instant nextExecutionTime) {
        // Hint that a new execution was scheduled in-case we want to go check for it immediately
        earlyExecutionListener.newEvent(
                new ClientEvent(new ClientEventContext(EventType.RESCHEDULE, taskInstanceId, nextExecutionTime)));
    }
}
