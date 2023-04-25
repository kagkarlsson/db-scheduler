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
package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static com.github.kagkarlsson.scheduler.task.helper.Tasks.DEFAULT_RETRY_INTERVAL;

public class AsyncOneTimeTaskMain extends Example {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncOneTimeTaskMain.class);

    public static void main(String[] args) {
        new AsyncOneTimeTaskMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        int iterations = 100;
        CountDownLatch countdown = new CountDownLatch(iterations);

        AsyncTask<Void> task = new AsyncTask<>("async-test", Void.class,
            (taskInstance, executionContext) -> CompletableFuture.supplyAsync(() -> {
                LOG.info("Executing " + taskInstance.getId());
                return new CompletionHandler<Void>() {
                    @Override
                    public void complete(ExecutionComplete executionComplete, ExecutionOperations<Void> executionOperations) {
                        executionOperations.remove();
                        countdown.countDown();
                        LOG.info("Completed " + executionComplete.getExecution().taskInstance.getId());
                    }
                };
            }, executionContext.getAsyncExecutor()));

        final Scheduler scheduler = Scheduler
            .create(dataSource, task)
            .threads(2)
            .build();

        // Schedule the task for execution a certain time in the future and optionally provide custom data for the execution
        for (int i = 0; i < iterations; i++) {
            scheduler.schedule(task.instance(String.valueOf(i)), Instant.now());
        }

        scheduler.start();

        try {
            countdown.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Done!");
        scheduler.stop();
    }

    public static class AsyncTask<T> extends AbstractTask<T> implements AsyncExecutionHandler<T> {
        private final AsyncExecutionHandler<T> handler;

        public AsyncTask(String name, Class<T> dataClass, AsyncExecutionHandler<T> handler) {
            super(name,
                dataClass,
                new FailureHandler.OnFailureRetryLater<>(DEFAULT_RETRY_INTERVAL),
                new DeadExecutionHandler.ReviveDeadExecution<T>());
            this.handler = handler;
        }

        @Override
        public CompletableFuture<CompletionHandler<T>> executeAsync(TaskInstance<T> taskInstance, AsyncExecutionContext executionContext) {
            return handler.executeAsync(taskInstance, executionContext);
        }

        @Override
        public SchedulableInstance<T> schedulableInstance(String id) {
            return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id), (currentTime) -> currentTime);
        }

        @Override
        public SchedulableInstance<T> schedulableInstance(String id, T data) {
            return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id, data), (currentTime) -> currentTime);
        }

    }
}
