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
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerListeners implements SchedulerListener {
  public static final SchedulerListeners NOOP = new SchedulerListeners(List.of());
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerListeners.class);

  private List<SchedulerListener> schedulerListeners;

  public SchedulerListeners(List<SchedulerListener> schedulerListeners) {
    this.schedulerListeners = schedulerListeners;
  }

  public void add(SchedulerListener listener) {
    schedulerListeners =
        Stream.concat(schedulerListeners.stream(), Stream.of(listener))
            .collect(Collectors.toList());
  }

  public List<SchedulerListener> getSchedulerListeners() {
    return this.schedulerListeners;
  }

  @SuppressWarnings("CodeBlock2Expr")
  @Override
  public void onExecutionScheduled(TaskInstanceId taskInstanceId, Instant executionTime) {
    schedulerListeners.forEach(
        listener -> {
          fireAndLogErrors(
              listener,
              "onExecutionScheduled",
              () -> listener.onExecutionScheduled(taskInstanceId, executionTime));
        });
  }

  @SuppressWarnings("CodeBlock2Expr")
  @Override
  public void onExecutionStart(CurrentlyExecuting currentlyExecuting) {
    schedulerListeners.forEach(
        listener -> {
          fireAndLogErrors(
              listener, "onExecutionStart", () -> listener.onExecutionStart(currentlyExecuting));
        });
  }

  @SuppressWarnings("CodeBlock2Expr")
  @Override
  public void onExecutionComplete(ExecutionComplete executionComplete) {
    schedulerListeners.forEach(
        listener -> {
          fireAndLogErrors(
              listener,
              "onExecutionComplete",
              () -> listener.onExecutionComplete(executionComplete));
        });
  }

  @SuppressWarnings("CodeBlock2Expr")
  @Override
  public void onExecutionDead(Execution execution) {
    schedulerListeners.forEach(
        listener -> {
          fireAndLogErrors(listener, "onExecutionDead", () -> listener.onExecutionDead(execution));
        });
  }

  @SuppressWarnings("CodeBlock2Expr")
  @Override
  public void onExecutionFailedHeartbeat(CurrentlyExecuting currentlyExecuting) {
    schedulerListeners.forEach(
        listener -> {
          fireAndLogErrors(
              listener,
              "onExecutionFailedHeartbeat",
              () -> listener.onExecutionFailedHeartbeat(currentlyExecuting));
        });
  }

  @SuppressWarnings("CodeBlock2Expr")
  @Override
  public void onSchedulerEvent(SchedulerEventType type) {
    schedulerListeners.forEach(
        listener -> {
          fireAndLogErrors(listener, "onSchedulerEvent", () -> listener.onSchedulerEvent(type));
        });
  }

  @SuppressWarnings("CodeBlock2Expr")
  @Override
  public void onCandidateEvent(CandidateEventType type) {
    schedulerListeners.forEach(
        listener -> {
          fireAndLogErrors(listener, "onCandidateEvent", () -> listener.onCandidateEvent(type));
        });
  }

  public void fireAndLogErrors(SchedulerListener listener, String method, Runnable r) {
    try {
      r.run();
    } catch (Throwable e) {
      LOG.warn(
          "Listener '{}' method '{}' threw an unexpected Exception",
          listener.getClass().getName(),
          method);
    }
  }
}
