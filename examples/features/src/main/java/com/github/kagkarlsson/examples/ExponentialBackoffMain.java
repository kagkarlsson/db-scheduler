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

import static java.time.Duration.*;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.examples.helpers.ExampleHelpers;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Instant;
import javax.sql.DataSource;

public class ExponentialBackoffMain extends Example {

    public static void main(String[] args) {
        new ExponentialBackoffMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        OneTimeTask<Void> failingTask = Tasks.oneTime("exponential_backoff_task")
                .onFailure(new FailureHandler.ExponentialBackoffFailureHandler<>(ofSeconds(1)))
                .execute((taskInstance, executionContext) -> {
                    throw new RuntimeException("simulated task exception");
                });

        final Scheduler scheduler = Scheduler.create(dataSource, failingTask).pollingInterval(ofSeconds(2)).build();

        scheduler.schedule(failingTask.instance("1"), Instant.now());

        ExampleHelpers.registerShutdownHook(scheduler);

        scheduler.start();
    }
}
