/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.TaskRepository;

import java.time.Instant;

public abstract class FailureOperations<T> extends ExecutionOperations<T> {

    public FailureOperations(TaskRepository taskRepository, Execution execution) {
        super(taskRepository, execution);
    }

    public void rescheduleAndResetFailures(ExecutionComplete completed, Instant nextExecutionTime) {
        taskRepository.reschedule(execution, nextExecutionTime, execution.lastSuccess, completed.getTimeDone(), 0);
    }

    public void rescheduleAndResetFailures(ExecutionComplete completed, Instant nextExecutionTime, T newData) {
        taskRepository.reschedule(execution, nextExecutionTime, newData, execution.lastSuccess, completed.getTimeDone(), 0);
    }

}
