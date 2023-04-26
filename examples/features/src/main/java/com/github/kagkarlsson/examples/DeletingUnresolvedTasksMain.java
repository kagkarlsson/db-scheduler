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
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.IntStream;
import javax.sql.DataSource;

public class DeletingUnresolvedTasksMain extends Example {

    public static void main(String[] args) {
        new DeletingUnresolvedTasksMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        RecurringTask<Void> unresolvedTask = Tasks.recurring("unresolved1", Schedules.fixedDelay(Duration.ofSeconds(1)))
                .execute((taskInstance, executionContext) -> {
                    System.out.println("Ran");
                });
        RecurringTask<Void> unresolvedTask2 = Tasks
                .recurring("unresolved2", Schedules.fixedDelay(Duration.ofSeconds(1)))
                .execute((taskInstance, executionContext) -> {
                    System.out.println("Ran");
                });

        SchedulerClient client = SchedulerClient.Builder.create(dataSource).build();
        client.schedule(unresolvedTask.instance(RecurringTask.INSTANCE), Instant.now());
        client.schedule(unresolvedTask2.instance(RecurringTask.INSTANCE), Instant.now().plusSeconds(10));

        final Scheduler scheduler = Scheduler.create(dataSource).pollingInterval(Duration.ofSeconds(1))
                .heartbeatInterval(Duration.ofSeconds(5)).deleteUnresolvedAfter(Duration.ofSeconds(20)).build();

        ExampleHelpers.registerShutdownHook(scheduler);

        scheduler.start();

        IntStream.range(0, 5).forEach(i -> {
            scheduler.fetchScheduledExecutions(e -> {
            });
            scheduler.getFailingExecutions(Duration.ZERO);
        });
    }

}
