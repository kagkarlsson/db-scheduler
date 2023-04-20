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
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.TaskWithDataDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import utils.EventLogger;

@Configuration
public class JobChainingConfiguration {

    public static final TaskWithDataDescriptor<JobState> CHAINED_STEP_1_TASK =
            new TaskWithDataDescriptor<>("chained-step-1", JobState.class);
    public static final TaskWithDataDescriptor<JobState> CHAINED_STEP_2_TASK =
            new TaskWithDataDescriptor<>("chained-step-2", JobState.class);
    public static final TaskWithDataDescriptor<JobState> CHAINED_STEP_3_TASK =
            new TaskWithDataDescriptor<>("chained-step-3", JobState.class);
    private static int CHAINED_JOB_ID = 1;

    /** Start the example */
    public static void start(ExampleContext ctx) {
        ctx.log("Scheduling a chained one-time task to run.");

        int id = CHAINED_JOB_ID++;
        ctx.schedulerClient.schedule(CHAINED_STEP_1_TASK.instance("chain-" + id, new JobState(id, 0)), Instant.now());
    }

    /** Bean definition */
    @Bean
    public CustomTask<JobState> chainedStep1() {
        return Tasks.custom(CHAINED_STEP_1_TASK).execute((taskInstance, executionContext) -> {
            JobState data = taskInstance.getData();
            EventLogger.logTask(CHAINED_STEP_1_TASK, "Ran step1. Schedules step2 after successful run. Data: " + data);

            JobState nextJobState = new JobState(data.id, data.counter + 1);
            EventLogger.logTask(
                    CHAINED_STEP_1_TASK, "Ran step1. Schedules step2 after successful run. Data: " + nextJobState);
            return new CompletionHandler.OnCompleteReplace<>(CHAINED_STEP_2_TASK, nextJobState);
        });
    }

    /** Bean definition */
    @Bean
    public CustomTask<JobState> chainedStep2() {
        return Tasks.custom(CHAINED_STEP_2_TASK).execute((taskInstance, executionContext) -> {
            JobState data = taskInstance.getData();
            JobState nextJobState = new JobState(data.id, data.counter + 1);

            // simulate we are waiting for some condition to be fulfilled before continuing to next step
            if (nextJobState.counter >= 5) {
                EventLogger.logTask(
                        CHAINED_STEP_2_TASK,
                        "Ran step2. Condition met. Schedules final step (step3) after successful run. Data: " + data);
                return new CompletionHandler.OnCompleteReplace<>(CHAINED_STEP_3_TASK, nextJobState);
            } else {
                EventLogger.logTask(
                        CHAINED_STEP_2_TASK,
                        "Ran step2. Condition for progressing not yet met, rescheduling. Data: " + data);
                return new CompletionHandler.OnCompleteReschedule<>(
                        Schedules.fixedDelay(Duration.ofSeconds(5)), nextJobState);
            }
        });
    }

    /** Bean definition */
    @Bean
    public CustomTask<JobState> chainedStep3() {
        return Tasks.custom(CHAINED_STEP_3_TASK).execute((taskInstance, executionContext) -> {
            EventLogger.logTask(
                    CHAINED_STEP_3_TASK,
                    "Ran step3. This was the final step in the processing, removing. Data: " + taskInstance.getData());
            return new CompletionHandler.OnCompleteRemove<>(); // same as for one-time tasks
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
            return "JobState{" + "id=" + id + ", counter=" + counter + '}';
        }
    }
}
