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
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete.Result;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

public class OtelTracingListener extends AbstractSchedulerListener {

  private final OpenTelemetry openTelemetry;

  public OtelTracingListener(OpenTelemetry openTelemetry) {
    this.openTelemetry = openTelemetry;
  }

  @Override
  public void onExecutionStart(CurrentlyExecuting currentlyExecuting) {
    Tracer t = openTelemetry.getTracer("db-scheduler.execution");
    TaskInstance<?> taskInstance = currentlyExecuting.getTaskInstance();
    Span span =
        t.spanBuilder("execute")
            .setAttribute("task-name", taskInstance.getTaskName())
            .setAttribute("task-instance-id", taskInstance.getId())
            .startSpan();
    span.makeCurrent();
  }

  @Override
  public void onExecutionComplete(ExecutionComplete executionComplete) {
    Span currentSpan = Span.current();

    try {
      if (executionComplete.getResult() == Result.FAILED) {
        executionComplete.getCause().ifPresent(currentSpan::recordException);
      }
    } finally {
      currentSpan.end();
    }
  }
}
