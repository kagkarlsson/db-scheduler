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
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Deprecated
public class ComposableTask {

    public static <T> OneTimeTask<T> onetimeTask(String name, Class<T> dataClass, VoidExecutionHandler<T> executionHandler) {
        return new OneTimeTask<T>(name, dataClass) {
            @Override
            public CompletableFuture<Void> executeOnce(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
                return executionHandler.execute(taskInstance, executionContext);
            }
        };
    }

    public static <T> Task<T> customTask(String name, Class<T> dataClass, CompletionHandler<T> completionHandler, VoidExecutionHandler<T> executionHandler) {
        return new AbstractTask<T>(name, dataClass, new FailureHandler.OnFailureRetryLater<>(Duration.ofMinutes(5)), new DeadExecutionHandler.ReviveDeadExecution<>()) {
            @Override
            public SchedulableInstance<T> schedulableInstance(String id) {
                return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id), (currentTime) -> currentTime);
            }

            @Override
            public SchedulableInstance<T> schedulableInstance(String id, T data) {
                return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id, data), (currentTime) -> currentTime);
            }

            @Override
            public CompletableFuture<CompletionHandler<T>> execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
                return executionHandler.execute(taskInstance, executionContext)
                    .thenApply((v) -> completionHandler);
            }
        };
    }

    public static <T> Task<T> customTask(String name, Class<T> dataClass, CompletionHandler<T> completionHandler, FailureHandler<T> failureHandler, VoidExecutionHandler<T> executionHandler) {
        return new AbstractTask<T>(name, dataClass, failureHandler, new DeadExecutionHandler.ReviveDeadExecution<>()) {
            @Override
            public SchedulableInstance<T> schedulableInstance(String id) {
                return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id), (currentTime) -> currentTime);
            }

            @Override
            public SchedulableInstance<T> schedulableInstance(String id, T data) {
                return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id, data), (currentTime) -> currentTime);
            }

            @Override
            public CompletableFuture<CompletionHandler<T>> execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
                return executionHandler.execute(taskInstance, executionContext)
                    .thenApply((v) -> completionHandler);
            }
        };
    }

}
