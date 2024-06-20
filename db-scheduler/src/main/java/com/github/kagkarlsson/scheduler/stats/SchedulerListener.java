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
package com.github.kagkarlsson.scheduler.stats;

import com.github.kagkarlsson.scheduler.CurrentlyExecuting;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.CandidateStatsEvent;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;

public interface SchedulerListener {

  void onExecutionStart(CurrentlyExecuting currentlyExecuting);

  void onExecutionComplete(ExecutionComplete executionComplete);

  void onExecutionDead(Execution execution);

  void onExecutionFailedHeartbeat(CurrentlyExecuting currentlyExecuting);

  void onSchedulerEvent(SchedulerEventType type);

  void onCandidateEvent(CandidateEventType type);

  enum SchedulerEventType {
    UNEXPECTED_ERROR(SchedulerStatsEvent.UNEXPECTED_ERROR),
    FAILED_HEARTBEAT(SchedulerStatsEvent.FAILED_HEARTBEAT),
    COMPLETIONHANDLER_ERROR(SchedulerStatsEvent.COMPLETIONHANDLER_ERROR),
    FAILUREHANDLER_ERROR(SchedulerStatsEvent.FAILUREHANDLER_ERROR),
    DEAD_EXECUTION(SchedulerStatsEvent.DEAD_EXECUTION),
    RAN_UPDATE_HEARTBEATS(SchedulerStatsEvent.RAN_UPDATE_HEARTBEATS),
    RAN_DETECT_DEAD(SchedulerStatsEvent.RAN_DETECT_DEAD),
    RAN_EXECUTE_DUE(SchedulerStatsEvent.RAN_EXECUTE_DUE),
    FAILED_MULTIPLE_HEARTBEATS(SchedulerStatsEvent.FAILED_MULTIPLE_HEARTBEATS),
    UNRESOLVED_TASK(SchedulerStatsEvent.UNRESOLVED_TASK);

    private final SchedulerStatsEvent statsRegistryEvent;

    SchedulerEventType(SchedulerStatsEvent statsRegistryEvent) {
      this.statsRegistryEvent = statsRegistryEvent;
    }

    public SchedulerStatsEvent toStatsRegistryEvent() {
      return statsRegistryEvent;
    }
  }

  enum CandidateEventType {
    STALE(CandidateStatsEvent.STALE),
    ALREADY_PICKED(CandidateStatsEvent.ALREADY_PICKED),
    EXECUTED(CandidateStatsEvent.EXECUTED);

    private final CandidateStatsEvent statsRegistryEvent;

    CandidateEventType(CandidateStatsEvent statsRegistryEvent) {
      this.statsRegistryEvent = statsRegistryEvent;
    }

    public CandidateStatsEvent toStatsRegistryEvent() {
      return statsRegistryEvent;
    }
  }

  SchedulerListener NOOP = new AbstractSchedulerListener() {};
}
