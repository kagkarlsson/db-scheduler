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
package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler.ReviveDeadExecution;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;

import java.time.Instant;

public abstract class RecurringTaskWithData extends Task<RecurringTaskWithData.TaskData> implements OnStartup {

    public static final String INSTANCE = "recurring";

    public RecurringTaskWithData(String name, Schedule schedule) {
        super(name, TaskData.class, new FailureHandler.OnFailureReschedule<>(schedule), new ReviveDeadExecution<>());
    }

    @Override
    public void onStartup(Scheduler scheduler) {
        scheduler.schedule(this.instance(INSTANCE), Instant.now());
    }

    @Override
    public CompletionHandler<RecurringTaskWithData.TaskData> execute(TaskInstance<RecurringTaskWithData.TaskData> taskInstance, ExecutionContext executionContext) {
        TaskData initialData = taskInstance.getData();
        TaskData nextData = new TaskData(initialData.counter + 1);
        // do stuff
        return new CompletionHandler<RecurringTaskWithData.TaskData>() {
            @Override
            public void complete(ExecutionComplete executionComplete, ExecutionOperations<RecurringTaskWithData.TaskData> executionOperations) {
                executionOperations.reschedule(
                        executionComplete,
                        executionComplete.getTimeDone().plusSeconds(60),
                        nextData);
            }
        };
    }


    public static class TaskData {
        public final int counter;

        public TaskData(int counter) {
            this.counter = counter;
        }
    }
}
