/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;

import java.time.Instant;

public class ScheduledExecution<DATA_TYPE> {

    private final Class<DATA_TYPE> dataClass;

    private final Execution execution;

    public ScheduledExecution(Class<DATA_TYPE> dataClass, Execution execution) {
        this.dataClass = dataClass;
        this.execution = execution;
    }

    public TaskInstanceId getTaskInstance() {
        return execution.taskInstance;
    }

    public Instant getExecutionTime() {
        return execution.getExecutionTime();
    }

    @SuppressWarnings("unchecked")
    public DATA_TYPE getData() {
        if (dataClass.isInstance(this.execution.taskInstance.getData())) {
            return (DATA_TYPE) this.execution.taskInstance.getData();
        }

        throw new DataClassMismatchException();
    }

    public static class DataClassMismatchException extends RuntimeException {

    }
}
