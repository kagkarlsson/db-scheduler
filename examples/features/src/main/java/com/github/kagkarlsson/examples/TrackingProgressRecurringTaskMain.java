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
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import java.io.Serializable;
import java.time.Duration;
import javax.sql.DataSource;

public class TrackingProgressRecurringTaskMain extends Example {

    public static void main(String[] args) {
        new TrackingProgressRecurringTaskMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {
        final FixedDelay schedule = FixedDelay.ofSeconds(2);

        final RecurringTask<Counter> statefulTask = Tasks.recurring("counting-task", schedule, Counter.class)
                .initialData(new Counter(0)).executeStateful((taskInstance, executionContext) -> {
                    final Counter startingCounter = taskInstance.getData();
                    for (int i = 0; i < 10; i++) {
                        System.out.println("Counting " + (startingCounter.value + i));
                    }
                    return new Counter(startingCounter.value + 10); // new value to be persisted as task_data for the
                                                                    // next run
                });

        final Scheduler scheduler = Scheduler.create(dataSource).pollingInterval(Duration.ofSeconds(5))
                .startTasks(statefulTask).registerShutdownHook().build();

        scheduler.start();
    }

    private static final class Counter implements Serializable {
        private final int value;

        public Counter(int value) {
            this.value = value;
        }
    }
}
