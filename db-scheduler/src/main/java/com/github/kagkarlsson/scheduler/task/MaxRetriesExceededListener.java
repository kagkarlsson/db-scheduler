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
package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.FailureHandler.MaxRetriesBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notified after max retries have been exceeded and the execution has been handled (rescheduled,
 * removed or similar). To take action over the terminal step instead, use {@link
 * MaxRetriesBuilder#then(FailureHandler)}.
 */
@FunctionalInterface
public interface MaxRetriesExceededListener {
  void onMaxRetriesExceeded(ExecutionComplete executionComplete);

  class OnMaxRetriesLogWarn implements MaxRetriesExceededListener {
    private static final Logger LOG = LoggerFactory.getLogger(MaxRetriesExceededListener.class);

    @Override
    public void onMaxRetriesExceeded(ExecutionComplete executionComplete) {
      LOG.warn(
          "Execution has failed max retries for task instance {}. Removing.",
          executionComplete.getExecution().taskInstance);
    }
  }
}
