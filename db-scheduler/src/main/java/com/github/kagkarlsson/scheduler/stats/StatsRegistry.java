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

public interface StatsRegistry {

    enum SchedulerStatsEvent {
        UNEXPECTED_ERROR,
        COMPLETIONHANDLER_ERROR,
        FAILUREHANDLER_ERROR,
        DEAD_EXECUTION,
        RAN_UPDATE_HEARTBEATS,
        RAN_DETECT_DEAD,
        RAN_EXECUTE_DUE,
        UNRESOLVED_TASK
    }

    enum CandidateStatsEvent {
        STALE,
        ALREADY_PICKED,
        EXECUTED
    }

    enum ExecutionStatsEvent {
        COMPLETED,
        FAILED
    }

    void register(SchedulerStatsEvent e);
    void register(CandidateStatsEvent e);
    void register(ExecutionStatsEvent e);

    void registerSingleCompletedExecution(ExecutionComplete completeEvent);

    StatsRegistry NOOP = new DefaultStatsRegistry();

    class DefaultStatsRegistry implements StatsRegistry {

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

        }
    }
}
