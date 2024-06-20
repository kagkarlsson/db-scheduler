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
package com.github.kagkarlsson.scheduler;

import static com.github.kagkarlsson.scheduler.ExceptionUtils.describe;

import com.github.kagkarlsson.scheduler.logging.ConfigurableLogger;
import com.github.kagkarlsson.scheduler.stats.SchedulerListener;
import com.github.kagkarlsson.scheduler.stats.SchedulerListener.CandidateEventType;
import com.github.kagkarlsson.scheduler.stats.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.Task;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
class ExecutePicked implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutePicked.class);
  private final Executor executor;
  private final TaskRepository taskRepository;
  private SchedulerClientEventListener earlyExecutionListener;
  private final SchedulerClient schedulerClient;
  private final SchedulerListener schedulerListener;
  private final TaskResolver taskResolver;
  private final SchedulerState schedulerState;
  private final ConfigurableLogger failureLogger;
  private final Clock clock;
  private HeartbeatConfig heartbeatConfig;
  private final Execution pickedExecution;

  public ExecutePicked(
      Executor executor,
      TaskRepository taskRepository,
      SchedulerClientEventListener earlyExecutionListener,
      SchedulerClient schedulerClient,
      SchedulerListener schedulerListener,
      TaskResolver taskResolver,
      SchedulerState schedulerState,
      ConfigurableLogger failureLogger,
      Clock clock,
      HeartbeatConfig heartbeatConfig,
      Execution pickedExecution) {
    this.executor = executor;
    this.taskRepository = taskRepository;
    this.earlyExecutionListener = earlyExecutionListener;
    this.schedulerClient = schedulerClient;
    this.schedulerListener = schedulerListener;
    this.taskResolver = taskResolver;
    this.schedulerState = schedulerState;
    this.failureLogger = failureLogger;
    this.clock = clock;
    this.heartbeatConfig = heartbeatConfig;
    this.pickedExecution = pickedExecution;
  }

  @Override
  public void run() {
    // FIXLATER: need to cleanup all the references back to scheduler fields
    CurrentlyExecuting currentlyExecuting =
        new CurrentlyExecuting(pickedExecution, clock, heartbeatConfig);
    final UUID executionId = executor.addCurrentlyProcessing(currentlyExecuting);

    try {
      schedulerListener.onCandidateEvent(CandidateEventType.EXECUTED);
      schedulerListener.onExecutionStart(currentlyExecuting);
      executePickedExecution(pickedExecution, currentlyExecuting);
    } finally {
      executor.removeCurrentlyProcessing(executionId);
    }
  }

  private void executePickedExecution(Execution execution, CurrentlyExecuting currentlyExecuting) {
    final Optional<Task> task = taskResolver.resolve(execution.taskInstance.getTaskName());
    if (!task.isPresent()) {
      LOG.error(
          "Failed to find implementation for task with name '{}'. Should have been excluded in JdbcRepository.",
          execution.taskInstance.getTaskName());
      schedulerListener.onSchedulerEvent(SchedulerEventType.UNEXPECTED_ERROR);
      return;
    }

    Instant executionStarted = clock.now();
    try {
      LOG.debug("Executing: " + execution);
      CompletionHandler completion =
          task.get()
              .execute(
                  execution.taskInstance,
                  new ExecutionContext(
                      schedulerState, execution, schedulerClient, currentlyExecuting));
      LOG.debug("Execution done: " + execution);

      complete(completion, execution, executionStarted);

    } catch (RuntimeException unhandledException) {
      failure(task.get(), execution, unhandledException, executionStarted, "Unhandled exception");

    } catch (Throwable unhandledError) {
      failure(task.get(), execution, unhandledError, executionStarted, "Error");
    }
  }

  private void complete(
      CompletionHandler completion, Execution execution, Instant executionStarted) {
    ExecutionComplete completeEvent =
        ExecutionComplete.success(execution, executionStarted, clock.now());
    try {
      completion.complete(
          completeEvent,
          new ExecutionOperations(taskRepository, earlyExecutionListener, execution));
    } catch (Throwable e) {
      schedulerListener.onSchedulerEvent(SchedulerEventType.COMPLETIONHANDLER_ERROR);
      schedulerListener.onSchedulerEvent(SchedulerEventType.UNEXPECTED_ERROR);
      LOG.error(
          "Failed while completing execution {}, because {}. Execution will likely remain scheduled and locked/picked. "
              + "The execution should be detected as dead after a while, and handled according to the tasks DeadExecutionHandler.",
          execution,
          describe(e),
          e);
    } finally {
      schedulerListener.onExecutionComplete(completeEvent);
    }
  }

  private void failure(
      Task task,
      Execution execution,
      Throwable cause,
      Instant executionStarted,
      String errorMessagePrefix) {
    String logMessage = "{} {} during execution of task with name '{}'. Treating as failure.";
    failureLogger.log(logMessage, cause, errorMessagePrefix, describe(cause), task.getName());

    ExecutionComplete completeEvent =
        ExecutionComplete.failure(execution, executionStarted, clock.now(), cause);
    try {
      task.getFailureHandler()
          .onFailure(
              completeEvent,
              new ExecutionOperations(taskRepository, earlyExecutionListener, execution));
    } catch (Throwable e) {
      schedulerListener.onSchedulerEvent(SchedulerEventType.FAILUREHANDLER_ERROR);
      schedulerListener.onSchedulerEvent(SchedulerEventType.UNEXPECTED_ERROR);
      LOG.error(
          "Failed while completing execution {}, because {}. Execution will likely remain scheduled and locked/picked. "
              + "The execution should be detected as dead after a while, and handled according to the tasks DeadExecutionHandler.",
          execution,
          describe(cause),
          e);
    } finally {
      schedulerListener.onExecutionComplete(completeEvent);
    }
  }
}
