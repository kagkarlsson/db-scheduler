package com.github.kagkarlsson.scheduler.stats;

import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.Task;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class MicrometerStatsRegistry implements StatsRegistry {

    private static final String RESULT_SUCCESS = "ok";
    private static final String RESULT_FAILURE = "failed";
    private final MeterRegistry meterRegistry;

    private Map<String, MetricsHolder> metricsMap = new HashMap<>();

    MicrometerStatsRegistry(MeterRegistry meterRegistry, List<? extends Task<?>> expectedTasks) {
        this.meterRegistry = meterRegistry;
        initializeMetricsForAllTasks(expectedTasks);
    }

    private void initializeMetricsForAllTasks(List<? extends Task<?>> expectedTasks) {
        expectedTasks.forEach(t -> metricsMap.put(t.getName(), new MetricsHolder(t.getName())));
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
        MetricsHolder metrics = getMetrics(completeEvent.getExecution().taskInstance.getTaskName());
        metrics.registerExecution(completeEvent);
    }

    private synchronized MetricsHolder getMetrics(String taskName) {
        return metricsMap.get(taskName);
    }

    private class MetricsHolder {
        private final AtomicReference<Double> lastDurationForTask = new AtomicReference<>((double) 0);
        private final AtomicLong lastRunTimestampForTask = new AtomicLong(0);
        private final Counter successesForTask;
        private final Counter failuresForTask;

        MetricsHolder(String taskName) {
            Gauge.builder("dbscheduler_task_last_run_duration_seconds", lastDurationForTask::get)
                .description("Duration in seconds for last execution of this task")
                .tag("task", taskName)
                .register(meterRegistry);

            Gauge.builder("dbscheduler_task_last_run_timestamp_seconds", lastRunTimestampForTask::get)
                .description("Time when last run completed")
                .tag("task", taskName)
                .register(meterRegistry);

            successesForTask = Counter.builder("dbscheduler_task_completions_total")
                .description("Successes and failures by task")
                .tag("task", taskName)
                .tag("result", RESULT_SUCCESS)
                .register(meterRegistry);

            failuresForTask = Counter.builder("dbscheduler_task_completions_total")
                .description("Successes and failures by task")
                .tag("task", taskName)
                .tag("result", RESULT_FAILURE)
                .register(meterRegistry);
        }

        void registerExecution(ExecutionComplete completeEvent) {
            lastDurationForTask.set((completeEvent.getDuration().toNanos()) / 1E9);
            lastRunTimestampForTask.set(completeEvent.getTimeDone().getEpochSecond());

            if (completeEvent.getResult() == ExecutionComplete.Result.OK) {
                successesForTask.increment();
            } else {
                failuresForTask.increment();
            }

        }
    }
}
