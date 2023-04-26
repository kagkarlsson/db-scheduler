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
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Instant;
import javax.sql.DataSource;

public class SchedulerClientMain extends Example {

    public static void main(String[] args) {
        new SchedulerClientMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        final OneTimeTask<Void> task = Tasks.oneTime("task-a").execute((taskInstance, executionContext) -> {
            System.out.println("Task a executed");
        });

        final SchedulerClient client = SchedulerClient.Builder.create(dataSource, task).build();

        final Instant now = Instant.now();
        for (int i = 0; i < 5; i++) {
            client.schedule(task.instance("id" + i), now.plusSeconds(i));
        }

        System.out.println("Listing scheduled executions");
        client.getScheduledExecutions(ScheduledExecutionsFilter.all()).forEach(execution -> {
            System.out.printf("Scheduled execution: taskName=%s, instance=%s, executionTime=%s%n",
                    execution.getTaskInstance().getTaskName(), execution.getTaskInstance().getId(),
                    execution.getExecutionTime());
        });

    }
}
