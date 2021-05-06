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
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

class ScheduleRecurringOnStartup<T> implements ScheduleOnStartup<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleRecurringOnStartup.class);
    private final Schedule schedule;
    private final String instance;
    private final T data;

    ScheduleRecurringOnStartup(String instance, T data, Schedule schedule) {
        this.instance = instance;
        this.data = data;
        this.schedule = schedule;
    }

    @Override
    public void apply(SchedulerClient scheduler, Clock clock, Task<T> task) {
        final TaskInstance<T> instanceWithoutData = task.instance(this.instance);
        final Optional<ScheduledExecution<Object>> preexistingExecution = scheduler.getScheduledExecution(instanceWithoutData);

        if (preexistingExecution.isPresent()) {
            Optional<Instant> newNextExecutionTime = checkForNewExecutionTime(clock, instanceWithoutData, preexistingExecution.get());

            newNextExecutionTime.ifPresent(instant -> {
                // Intentionally leaving out data here, since we do not want to update existing data
                scheduler.reschedule(instanceWithoutData, instant);
            });

        } else {
            // No preexisting execution, create initial one
            final Instant initialExecutionTime = schedule.getInitialExecutionTime(clock.now());
            LOG.info("Creating initial execution for task-instance '{}'. Next execution-time: {}", instanceWithoutData, initialExecutionTime);
            scheduler.schedule(getSchedulableInstance(task), initialExecutionTime);
        }
    }

    Optional<Instant> checkForNewExecutionTime(Clock clock, TaskInstance<T> instanceWithoutData, ScheduledExecution<Object> preexistingExecution) {
        final Instant preexistingExecutionTime = preexistingExecution.getExecutionTime();
        final Instant nextExecutionTimeRelativeToNow = schedule.getNextExecutionTime(ExecutionComplete.simulatedSuccess(clock.now()));

        if (preexistingExecutionTime.isBefore(clock.now().plus(Duration.ofSeconds(10)))) {
            // Only touch future executions as a precaution to avoid warnings/noise related to execution being run by the scheduler
            LOG.debug("Not checking if task-instance '{}' needs rescheduling as its execution-time is too near now().", instanceWithoutData);
            return Optional.empty();

        } else if (schedule.isDeterministic()) {
            final Instant potentiallyNewExecutionTime = schedule.getNextExecutionTime(ExecutionComplete.simulatedSuccess(clock.now()));
            // Schedules: Cron, Daily
            if (differenceGreaterThan(preexistingExecutionTime, potentiallyNewExecutionTime, Duration.ofSeconds(1))) {
                // Deterministic schedule must have changed (note: may be nuances here depending on precision of database timestamp)
                LOG.info("Rescheduling task-instance '{}' because deterministic Schedule seem to have been updated. " +
                        "Previous execution-time: {}, new execution-time: {}",
                    instanceWithoutData, preexistingExecutionTime, potentiallyNewExecutionTime);
                return Optional.of(potentiallyNewExecutionTime);
            }

        } else if (preexistingExecutionTime.isAfter(nextExecutionTimeRelativeToNow)) {
            // Schedules: FixedDelay
            // The schedule has likely been updated, rescheduling to next execution-time according to schedule
            // We cannot reschedule if new execution-time is further into the future than existing since that may cause
            // us to never run executions if scheduler restarts is more frequent than the fixed-delay duration

            LOG.info("Rescheduling task-instance '{}' because non-deterministic Schedule seem to have been updated to " +
                    "a more frequent one. Previous execution-time: {}, new execution-time: {}",
                instanceWithoutData, preexistingExecutionTime, nextExecutionTimeRelativeToNow);
            return Optional.of(nextExecutionTimeRelativeToNow);

        }

        LOG.debug("Task-instance '{}' is already scheduled, skipping schedule-on-startup.", instanceWithoutData);
        return Optional.empty();
    }

    static boolean differenceGreaterThan(Instant preexistingExecutionTime, Instant potentiallyNewExecutionTime, Duration delta) {
        final Duration difference = Duration.between(preexistingExecutionTime, potentiallyNewExecutionTime).abs();
        return difference.toMillis() > delta.toMillis();
    }

    private TaskInstance<T> getSchedulableInstance(Task<T> task) {
        if (data == null) {
            return task.instance(instance);
        } else {
            return task.instance(instance, data);
        }
    }

}
