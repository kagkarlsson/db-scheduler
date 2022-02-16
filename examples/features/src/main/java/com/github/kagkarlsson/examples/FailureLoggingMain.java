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
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;

import javax.sql.DataSource;

public class FailureLoggingMain extends Example {

    public static void main(String[] args) {
        new FailureLoggingMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        RecurringTask<Void> hourlyTask = Tasks.recurring("my-failing-task", FixedDelay.ofSeconds(10))
            .execute((inst, ctx) -> {
                throw new RuntimeException("Simulated failure!");
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource)
            .startTasks(hourlyTask)
            .failureLogging(LogLevel.INFO, true)
            .build();

        // hourlyTask is automatically scheduled on startup if not already started (i.e. exists in the db)
        scheduler.start();
    }
}
