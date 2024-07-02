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
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.ExecutionStatsEvent;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete.Result;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;

public class StatsRegistryAdapter implements SchedulerListener {

  private final StatsRegistry statsRegistry;

  public StatsRegistryAdapter(StatsRegistry statsRegistry) {
    this.statsRegistry = statsRegistry;
  }

  @Override
  public void onExecutionScheduled(TaskInstanceId taskInstanceId, Instant executionTime) {}

  @Override
  public void onExecutionStart(CurrentlyExecuting currentlyExecuting) {}

  @Override
  public void onExecutionComplete(ExecutionComplete executionComplete) {
    if (statsRegistry == null) {
      return;
    }

    if (executionComplete.getResult() == Result.OK) {
      statsRegistry.register(ExecutionStatsEvent.COMPLETED);
    } else {
      statsRegistry.register(ExecutionStatsEvent.FAILED);
    }
    statsRegistry.registerSingleCompletedExecution(executionComplete);
  }

  @Override
  public void onExecutionDead(Execution execution) {}

  @Override
  public void onExecutionFailedHeartbeat(CurrentlyExecuting currentlyExecuting) {}

  @Override
  public void onSchedulerEvent(SchedulerEventType type) {
    if (statsRegistry == null) {
      return;
    }
    statsRegistry.register(type.toStatsRegistryEvent());
  }

  @Override
  public void onCandidateEvent(CandidateEventType type) {
    if (statsRegistry == null) {
      return;
    }
    statsRegistry.register(type.toStatsRegistryEvent());
  }
}
