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
package com.github.kagkarlsson.scheduler.task.helper;

import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.CompletionHandler.OnCompleteRemove;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler.ReviveDeadExecution;
import com.github.kagkarlsson.scheduler.task.FailureHandler.OnFailureRetryLater;

import java.time.Duration;
import java.time.Instant;

public abstract class OneTimeTask<T> extends AbstractTask<T> {

    public OneTimeTask(String name, Class<T> dataClass) {
        this(name, dataClass, new OnFailureRetryLater<>(Duration.ofMinutes(5)), new ReviveDeadExecution<>());
    }

    public OneTimeTask(String name, Class<T> dataClass, FailureHandler<T> failureHandler, DeadExecutionHandler<T> deadExecutionHandler) {
        super(name, dataClass, failureHandler, deadExecutionHandler);
    }

    @Override
    public SchedulableInstance<T> schedulableInstance(String id) {
        return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id), (currentTime) -> currentTime);
    }

    @Override
    public SchedulableInstance<T> schedulableInstance(String id, T data) {
        return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id, data), (currentTime) -> currentTime);
    }

    @Override
    public CompletionHandler<T> execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
        executeOnce(taskInstance, executionContext);
        return new OnCompleteRemove<>();
    }

    public abstract void executeOnce(TaskInstance<T> taskInstance, ExecutionContext executionContext);

    @Override
    public String toString() {
        return "OneTimeTask name=" + getName();
    }

}
