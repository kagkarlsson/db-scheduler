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
package com.github.kagkarlsson.scheduler.task.helper;

import com.github.kagkarlsson.scheduler.Clock;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

class ScheduleRecurringOnStartup<T> implements ScheduleOnStartup<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleRecurringOnStartup.class);
    private Schedule schedule;
    private String instance;
    private T data;

    ScheduleRecurringOnStartup(String instance, T data, Schedule schedule) {
        this.instance = instance;
        this.data = data;
        this.schedule = schedule;
    }

    @Override
    public void apply(Scheduler scheduler, Clock clock, Task<T> task) {
        final TaskInstance<T> instanceWithoutData = task.instance(this.instance);
        final Optional<ScheduledExecution<Object>> preexistingExecution = scheduler.getScheduledExecution(instanceWithoutData);

        if (preexistingExecution.isPresent()) {
            final Instant nextExecutionTime = schedule.getNextExecutionTime(ExecutionComplete.simulatedSuccess(clock.now()));

            if (preexistingExecution.get().getExecutionTime().isAfter(nextExecutionTime)) {
                // the schedule has likely been updated, rescheduling to next execution-time according to schedule

                LOG.info("Rescheduling task-instance '{}' because Schedule seem to have been updated. " +
                        "Previous execution-time: {}, new execution-time: {}",
                    instanceWithoutData, preexistingExecution.get().getExecutionTime(), nextExecutionTime);
                // Intentionally leaving out data here, since we do not want to update existing data
                scheduler.reschedule(instanceWithoutData, nextExecutionTime);

            } else {
                LOG.debug("Task-instance '{}' is already scheduled, skipping schedule-on-startup.", instanceWithoutData);
            }

        } else {
            final Instant initialExecutionTime = schedule.getInitialExecutionTime(clock.now());
            LOG.info("Creating initial execution for task-instance '{}'. Next execution-time: {}", instanceWithoutData, initialExecutionTime);
            scheduler.schedule(getSchedulableInstance(task), initialExecutionTime);
        }
    }

    private TaskInstance<T> getSchedulableInstance(Task<T> task) {
        if (data == null) {
            return task.instance(instance);
        } else {
            return task.instance(instance, data);
        }
    }

}
