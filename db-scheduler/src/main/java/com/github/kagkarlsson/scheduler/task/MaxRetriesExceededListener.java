package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.FailureHandler.MaxRetriesBuilder;

/**
 * Notified after max retries have been exceeded and the execution has been handled (rescheduled,
 * removed or similar). To take action over the terminal step instead, use {@link
 * MaxRetriesBuilder#then(FailureHandler)}.
 */
@FunctionalInterface
public interface MaxRetriesExceededListener {
  void onMaxRetriesExceeded(ExecutionComplete executionComplete);
}
