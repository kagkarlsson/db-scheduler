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
package com.github.kagkarlsson.scheduler.event;

import com.github.kagkarlsson.scheduler.CurrentlyExecuting;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.CandidateStatsEvent;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;

/**
 * The method-parameters might be subject to change. For instance, Event-types might be introduced
 * to hold the data relevant to the event.
 *
 * <p>Will typically run in the same Thread as the execution, so must not do I/O or similar slow
 * operations.
 */
public interface SchedulerListener {

  /**
   * Execution scheduled either by the <code>SchedulerClient</code> or by a <code>CompletionHandler
   * </code>
   *
   * @param taskInstanceId
   * @param executionTime
   */
  void onExecutionScheduled(TaskInstanceId taskInstanceId, Instant executionTime);

  /**
   * Execution picked and <code>ExecutionHandler</code> about to run. Will typically run in the same
   * thread as <code>onExecutionComplete</code>
   *
   * @param currentlyExecuting
   */
  void onExecutionStart(CurrentlyExecuting currentlyExecuting);

  /**
   * <code>ExecutionHandler</code> done. Will typically run in the same thread as <code>
   * onExecutionStart</code>
   *
   * @param executionComplete
   */
  void onExecutionComplete(ExecutionComplete executionComplete);

  /**
   * Scheduler has detected a <i>dead</i> execution. About to run <code>DeadExecutionHandler</code>.
   *
   * @param execution
   */
  void onExecutionDead(Execution execution);

  /**
   * Scheduler failed to update heartbeat-timestamp for execution that it is currently executing.
   * Multiple failures will eventually lead to the execution being declared <i>dead</i> (and its
   * <code>DeadExecutionHandler</code> triggered).
   *
   * @param currentlyExecuting
   */
  void onExecutionFailedHeartbeat(CurrentlyExecuting currentlyExecuting);

  /**
   * Internal scheduler event. Primarily intended for testing.
   *
   * @param type
   */
  void onSchedulerEvent(SchedulerEventType type);

  /**
   * Internal scheduler event. Primarily intended for testing.
   *
   * @param type
   */
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
