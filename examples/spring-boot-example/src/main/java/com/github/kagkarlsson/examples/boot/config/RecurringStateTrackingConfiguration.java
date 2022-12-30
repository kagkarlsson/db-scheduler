/**
 * Copyright (C) Gustav Karlsson
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import utils.EventLogger;
import utils.Utils;

import java.time.Instant;
import java.util.Random;

import static com.github.kagkarlsson.examples.boot.config.TaskNames.*;

@Configuration
public class RecurringStateTrackingConfiguration {

    @Bean
    public Task<Integer> stateTrackingRecurring() {
        return Tasks.recurring(STATE_TRACKING_RECURRING_TASK, Schedules.cron("0/5 * * * * *"))
            .doNotScheduleOnStartup() // just for demo-purposes, so it does not start with the demo-application
            .executeStateful((TaskInstance<Integer> taskInstance, ExecutionContext executionContext) -> {
                EventLogger.logTask(STATE_TRACKING_RECURRING_TASK, "Ran recurring task. Will keep running according to the same schedule, " +
                    "but the state is updated. State: " + taskInstance.getData());

                // Stateful recurring return the updated state as the final step (convenience)
                return taskInstance.getData() + 1;
            });
    }

}
