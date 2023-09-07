package com.github.kagkarlsson.scheduler.concurrent;

import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecordResultAndStopExecutionOnComplete<T> implements CompletionHandler<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(RecordResultAndStopExecutionOnComplete.class);

  final List<String> ok = Collections.synchronizedList(new ArrayList<>());
  final List<String> failed = Collections.synchronizedList(new ArrayList<>());
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
