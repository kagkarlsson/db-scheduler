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
package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.examples.boot.ExampleContext;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskWithoutDataDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Instant;
import java.util.Random;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.TransactionStatus;
import utils.EventLogger;

@Configuration
public class TransactionallyStagedJobConfiguration {

    public static final TaskWithoutDataDescriptor TRANSACTIONALLY_STAGED_TASK =
            new TaskWithoutDataDescriptor("transactionally-staged-task");
    private static int ID = 1;

    /** Start the example */
    public static void start(ExampleContext ctx) {
        ctx.log(
                "Scheduling a one-time task in a transaction. If the transaction rolls back, the insert of the task also "
                        + "rolls back, i.e. it will never run.");

        ctx.tx.executeWithoutResult((TransactionStatus status) -> {
            // Since it is scheduled in a transaction, the scheduler will not run it until the tx commits
            // If the tx rolls back, the insert of the new job will also roll back, i.e. not run.
            ctx.schedulerClient.schedule(TRANSACTIONALLY_STAGED_TASK.instance(String.valueOf(ID++)), Instant.now());

            // Do additional database-operations here

            if (new Random().nextBoolean()) {
                throw new RuntimeException(
                        "Simulated failure happening after task was scheduled. Scheduled task will never run.");
            }
        });
    }

    /** Bean definition */
    @Bean
    public Task<Void> transactionallyStagedTask() {
        return Tasks.oneTime(TRANSACTIONALLY_STAGED_TASK)
                .execute((TaskInstance<Void> taskInstance, ExecutionContext executionContext) -> {
                    EventLogger.logTask(
                            TRANSACTIONALLY_STAGED_TASK,
                            "Ran. Will only run if transactions it was scheduled commits. ");
                });
    }
}
