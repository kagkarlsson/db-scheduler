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
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.exceptions.DataClassMismatchException;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.Objects;

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
        Object data = this.execution.taskInstance.getData();
        if (data == null) {
            return null;
        } else if (dataClass.isInstance(data)) {
            return (DATA_TYPE) data;
        }
        throw new DataClassMismatchException(dataClass, data.getClass());
    }

    public Instant getLastSuccess() {
        return execution.lastSuccess;
    }

    public Instant getLastFailure() {
        return execution.lastFailure;
    }

    public int getConsecutiveFailures() {
        return execution.consecutiveFailures;
    }

    public boolean isPicked() {
        return execution.picked;
    }

    public String getPickedBy() {
        return execution.pickedBy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ScheduledExecution<?> that = (ScheduledExecution<?>) o;
        return Objects.equals(execution, that.execution);
    }

    @Override
    public int hashCode() {
        return Objects.hash(execution);
    }

    @Override
    public String toString() {
        return "ScheduledExecution{" + "execution=" + execution + '}';
    }
}
