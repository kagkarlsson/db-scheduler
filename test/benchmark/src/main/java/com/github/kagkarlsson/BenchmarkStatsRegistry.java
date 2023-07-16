/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
