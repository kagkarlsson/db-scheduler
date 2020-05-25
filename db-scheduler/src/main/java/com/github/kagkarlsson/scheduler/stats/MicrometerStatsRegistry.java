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
package com.github.kagkarlsson.scheduler.stats;

import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.Task;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MicrometerStatsRegistry implements StatsRegistry {

    private static final String RESULT_SUCCESS = "ok";
    private static final String RESULT_FAILURE = "failed";
    private final MeterRegistry meterRegistry;

    private Map<String, MetricsHolder> metricsMap = new HashMap<>();

    public MicrometerStatsRegistry(MeterRegistry meterRegistry, List<? extends Task<?>> expectedTasks) {
        this.meterRegistry = meterRegistry;
        initializeMetricsForAllTasks(expectedTasks);
    }

    private void initializeMetricsForAllTasks(List<? extends Task<?>> expectedTasks) {
        expectedTasks.forEach(task -> getOrInitMetricHolder(task.getName()));
    }

    private MetricsHolder getOrInitMetricHolder(String taskName) {
        MetricsHolder preSynchronizedValue = metricsMap.get(taskName);
        if (preSynchronizedValue != null) {
            return preSynchronizedValue;
        } else {
            // Thread-safe init
            synchronized (this) {
                MetricsHolder currentValue = metricsMap.get(taskName);
                if (currentValue == null) {
                    currentValue = new MetricsHolder(taskName);
                    metricsMap.put(taskName, currentValue);
                }
                return currentValue;
            }
        }

    }

    @Override
    public void register(SchedulerStatsEvent e) {

    }

    @Override
    public void register(CandidateStatsEvent e) {

    }

    @Override
    public void register(ExecutionStatsEvent e) {

    }

    @Override
    public void registerSingleCompletedExecution(ExecutionComplete completeEvent) {
        String taskName = completeEvent.getExecution().taskInstance.getTaskName();
        MetricsHolder metrics = getOrInitMetricHolder(taskName);
        metrics.registerExecution(completeEvent);
    }

    private class MetricsHolder {
        private final AtomicReference<Double> lastDurationForTask = new AtomicReference<>((double) 0);
        private final AtomicLong lastRunTimestampForTask = new AtomicLong(0);
        private final Counter successesForTask;
        private final Counter failuresForTask;
        private final Timer durationsForTask;

        MetricsHolder(String taskName) {
            Gauge.builder("dbscheduler_task_last_run_duration", lastDurationForTask::get)
                .description("Duration in seconds for last execution of this task")
                .tag("task", taskName)
                .register(meterRegistry);

            Gauge.builder("dbscheduler_task_last_run_timestamp_seconds", lastRunTimestampForTask::get)
                .description("Time when last run completed")
                .tag("task", taskName)
                .register(meterRegistry);

            successesForTask = Counter.builder("dbscheduler_task_completions")
                .description("Successes and failures by task")
                .tag("task", taskName)
                .tag("result", RESULT_SUCCESS)
                .register(meterRegistry);

            failuresForTask = Counter.builder("dbscheduler_task_completions")
                .description("Successes and failures by task")
                .tag("task", taskName)
                .tag("result", RESULT_FAILURE)
                .register(meterRegistry);

            durationsForTask = Timer.builder("dbscheduler_task_duration_total")
                .description("Duration of executions")
                .tag("task", taskName)
                .register(meterRegistry);
        }

        void registerExecution(ExecutionComplete completeEvent) {
            lastDurationForTask.set((completeEvent.getDuration().toNanos()) / 1E9);
            lastRunTimestampForTask.set(completeEvent.getTimeDone().getEpochSecond());

            durationsForTask.record(completeEvent.getDuration().toMillis(), TimeUnit.MILLISECONDS);
            if (completeEvent.getResult() == ExecutionComplete.Result.OK) {
                successesForTask.increment();
            } else {
                failuresForTask.increment();
            }

        }
    }
}
