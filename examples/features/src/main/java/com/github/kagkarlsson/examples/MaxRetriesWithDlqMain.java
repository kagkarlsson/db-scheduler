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
package com.github.kagkarlsson.examples;

import static com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter.deactivated;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * After {@code maxRetries} failed attempts, {@code thenDeactivate(State.FAILED)} parks the
 * execution in the FAILED state instead of removing it. The deactivated rows form a
 * dead-letter-queue you can inspect via {@link ScheduledExecutionsFilter#deactivated()} and revive
 * with {@code client.reactivate(...)} once the underlying issue is fixed.
 */
public class MaxRetriesWithDlqMain extends Example {
  public static final TaskDescriptor<Void> MAX_RETRIES_TASK = TaskDescriptor.of("max_retries_task");
  private static final Logger LOG = LoggerFactory.getLogger(MaxRetriesWithDlqMain.class);
  private final MightThrow mightThrow = new MightThrow();

  public static void main(String[] args) {
    new MaxRetriesWithDlqMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {

    OneTimeTask<Void> failingTask =
        Tasks.oneTime(MAX_RETRIES_TASK)
            .onFailure(
                FailureHandler.<Void>maxRetries(3)
                    .retryEvery(Duration.ofSeconds(2))
                    .thenDeactivate(State.FAILED))
            .execute(
                (taskInstance, executionContext) -> {
                  LOG.info("Attempting execution");
                  mightThrow.run();
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, failingTask)
            .pollingInterval(Duration.ofSeconds(1))
            .registerShutdownHook()
            .build();

    var instance = MAX_RETRIES_TASK.instance("1").scheduledTo(Instant.now());
    scheduler.schedule(instance);
    scheduler.start();

    // exchaust attempts
    sleep(12_000);

    LOG.info(">>> Deactivated executions:");
    var dlq = scheduler.getScheduledExecutions(deactivated());
    for (ScheduledExecution<Object> e : dlq) {
      LOG.info(
          "    {} state={} consecutiveFailures={}",
          e.getTaskInstance().getId(),
          e.getState(),
          e.getConsecutiveFailures());
    }

    // Reactivate after issue fixed
    LOG.info(">>> Reactivating from DLQ");
    mightThrow.setFailing(false);
    scheduler.reactivate(instance, Instant.now());
  }

  static class MightThrow implements Runnable {
    private boolean failing = true;

    @Override
    public void run() {
      if (failing) {
        throw new RuntimeException("simulated task exception");
      }
    }

    void setFailing(boolean failing) {
      this.failing = failing;
    }
  }
}
