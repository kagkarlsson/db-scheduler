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

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pause and resume a recurring task at runtime, e.g. for an operator who needs to temporarily stop
 * a noisy job without redeploying.
 */
public class PauseAndResumeRecurringTaskMain extends Example {
  private static final Logger LOG = LoggerFactory.getLogger(PauseAndResumeRecurringTaskMain.class);

  public static void main(String[] args) {
    new PauseAndResumeRecurringTaskMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {

    RecurringTask<Void> ticker =
        Tasks.recurring("ticker", FixedDelay.ofSeconds(1)).execute((inst, ctx) -> LOG.info("tick"));

    final Scheduler scheduler =
        Scheduler.create(dataSource)
            .startTasks(ticker)
            .pollingInterval(Duration.ofSeconds(1))
            .registerShutdownHook()
            .build();

    scheduler.start();

    sleep(5_000);
    LOG.info(">>> pausing ticker");
    scheduler.deactivate(ticker.getDefaultTaskInstance(), State.PAUSED);

    sleep(5_000);
    LOG.info(">>> resuming ticker");
    scheduler.reactivate(ticker.getDefaultTaskInstance(), Instant.now());

    sleep(5_000);
    scheduler.stop();
  }
}
