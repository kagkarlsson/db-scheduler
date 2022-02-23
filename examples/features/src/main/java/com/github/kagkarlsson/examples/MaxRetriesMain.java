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
import com.github.kagkarlsson.examples.helpers.ExampleHelpers;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;

public class MaxRetriesMain extends Example {

    public static void main(String[] args) {
        new MaxRetriesMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        OneTimeTask<Void> failingTask = Tasks.oneTime("max_retries_task")
            .onFailure(new FailureHandler.MaxRetriesFailureHandler<>(3, (executionComplete, executionOperations) -> {
                    // try again in 1 second
                    System.out.println("Execution has failed " + executionComplete.getExecution().consecutiveFailures + " times. Trying again in a bit...");
                    executionOperations.reschedule(executionComplete, Instant.now().plusSeconds(1));
            }))
            .execute((taskInstance, executionContext) -> {
                throw new RuntimeException("simulated task exception");
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource, failingTask)
            .pollingInterval(Duration.ofSeconds(2))
            .build();

        scheduler.schedule(failingTask.instance("1"), Instant.now());

        ExampleHelpers.registerShutdownHook(scheduler);

        scheduler.start();
    }

}
