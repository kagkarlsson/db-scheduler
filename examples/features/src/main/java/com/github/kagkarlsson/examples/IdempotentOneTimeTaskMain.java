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
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use {@code onCompleteDelayRemoval()} for at-most-once semantics over a time window. The completed
 * execution lingers in state {@link com.github.kagkarlsson.scheduler.task.State#COMPLETE COMPLETE}
 * so re-scheduling the same instance id during the window is a no-op.
 */
public class IdempotentOneTimeTaskMain extends Example {
  public static final TaskDescriptor<Void> WELCOME_EMAIL =
      TaskDescriptor.of("send-welcome-email", Void.class);
  private static final Logger LOG = LoggerFactory.getLogger(IdempotentOneTimeTaskMain.class);

  public static void main(String[] args) {
    new IdempotentOneTimeTaskMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {

    AtomicInteger executions = new AtomicInteger();

    OneTimeTask<Void> sendWelcome =
        Tasks.oneTime(WELCOME_EMAIL)
            .onCompleteDelayRemoval() // keep row in COMPLETE state after success
            .execute(
                (inst, ctx) -> {
                  int n = executions.incrementAndGet();
                  LOG.info("Sending welcome email to {} (execution #{})", inst.getId(), n);
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, sendWelcome)
            .pollingInterval(Duration.ofSeconds(1))
            .registerShutdownHook()
            .build();
    scheduler.start();

    String instanceId = "user-42";

    LOG.info(">>> first schedule (will run)");
    var instance = WELCOME_EMAIL.instance(instanceId).scheduledTo(Instant.now());
    scheduler.scheduleIfNotExists(instance);

    sleep(3_000);

    LOG.info(">>> simulate retry-attempt (will be de-duped");
    boolean scheduled = scheduler.scheduleIfNotExists(instance);
    LOG.info("    scheduleIfNotExists returned {}", scheduled);
    scheduler.stop();
  }
}
