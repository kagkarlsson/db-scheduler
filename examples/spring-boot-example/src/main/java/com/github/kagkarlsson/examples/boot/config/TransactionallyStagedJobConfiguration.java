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

import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import utils.EventLogger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;

import static com.github.kagkarlsson.examples.boot.config.TaskNames.TRANSACTIONALLY_STAGED_TASK;

@Configuration
public class TransactionallyStagedJobConfiguration {

    @Bean
    public Task<Void> transactionallyStagedTask() {
        return Tasks.oneTime(TRANSACTIONALLY_STAGED_TASK)
            .execute((TaskInstance<Void> taskInstance, ExecutionContext executionContext) -> {
                EventLogger.logTask(TRANSACTIONALLY_STAGED_TASK, "Ran. Will only run if transactions it was scheduled commits. ");
            });
    }

    public static class JobState implements Serializable {
        private static final long serialVersionUID = 1L; // recommended when using Java serialization
        public final int id;
        public final int counter;

        public JobState() {
            this(0, 0);
        } // for serializing

        public JobState(int id, int counter) {
            this.id = id;
            this.counter = counter;
        }

        @Override
        public String toString() {
            return "JobState{" +
                "id=" + id +
                ", counter=" + counter +
                '}';
        }
    }
}
