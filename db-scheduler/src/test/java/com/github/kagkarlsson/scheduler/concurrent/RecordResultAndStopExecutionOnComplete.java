package com.github.kagkarlsson.scheduler.concurrent;

import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecordResultAndStopExecutionOnComplete<T> implements CompletionHandler<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(RecordResultAndStopExecutionOnComplete.class);

  final ConcurrentLinkedQueue<String> ok = new ConcurrentLinkedQueue<>();
  final ConcurrentLinkedQueue<String> failed = new ConcurrentLinkedQueue<>();
  final Consumer<String> onComplete;

  RecordResultAndStopExecutionOnComplete(Consumer<String> onComplete) {
    this.onComplete = onComplete;
  }

  @Override
  public void complete(
      ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
    final String instanceId = executionComplete.getExecution().taskInstance.getId();
    try {
      if (executionComplete.getResult() == ExecutionComplete.Result.OK) {
        ok.add(instanceId);
      } else {
        failed.add(instanceId);
      }
      executionOperations.stop();

    } catch (Exception e) {
      LOG.error("Error during completion!", e);
    } finally {
      onComplete.accept(instanceId);
    }
  }
}
