package com.github.kagkarlsson;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;

public class BenchmarkStatsRegistry implements StatsRegistry {
    private final Meter executions;
    private final Meter unexpectedErrors;

    public BenchmarkStatsRegistry(MetricRegistry metrics) {
        executions = metrics.meter("executions");
        unexpectedErrors = metrics.meter("unexpected_errors");
    }

    @Override
    public void register(SchedulerStatsEvent e) {
        if (e == SchedulerStatsEvent.UNEXPECTED_ERROR) {
            unexpectedErrors.mark();
        }
    }

    @Override
    public void register(CandidateStatsEvent e) {

    }

    @Override
    public void register(ExecutionStatsEvent e) {

    }

    @Override
    public void registerSingleCompletedExecution(ExecutionComplete completeEvent) {
        executions.mark();
    }
}
