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
import com.github.kagkarlsson.examples.helpers.PersistentHsqlDatasource;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.DeactivateUpdate;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Liquibase-style one-shot migration: the task is seeded on startup and runs exactly once, ever.
 * After it completes it is kept indefinitely as a {@link State#RECORD} row. On the next startup,
 * the scheduler tries to seed it again, but the existing RECORD row blocks scheduling so the
 * migration body never runs a second time.
 *
 * <p>Uses a file-based HSQLDB ({@code ./target/db-scheduler-migration-demo*}) so DB state survives
 * JVM restarts. <b>Run {@code main} a second time</b> to observe the run-once-forever guarantee.
 */
public class MigrationStyleRecordMain extends Example {
  private static final Logger LOG = LoggerFactory.getLogger(MigrationStyleRecordMain.class);

  private static final String MIGRATION_INSTANCE_ID = "db-migration-v17";

  public static void main(String[] args) {
    new MigrationStyleRecordMain()
        .run(PersistentHsqlDatasource.initDatabase("db-scheduler-migration-demo"));
  }

  @Override
  public void run(DataSource dataSource) {

    // CustomTask is used because its builder exposes scheduleOnStartup(...), which seeds the
    // instance if it doesn't already exist. The execute(...) lambda returns a CompletionHandler
    // that deactivates the row to State.RECORD instead of removing it - leaving a permanent
    // "this has run" marker.
    // Likely we will create a `Tasks.record(..)` eventually to make this easier
    CustomTask<Void> migration =
        Tasks.custom("schema-migration", Void.class)
            .scheduleOnStartup(MIGRATION_INSTANCE_ID, null, instant -> instant)
            .execute(
                (taskInstance, executionContext) -> {
                  LOG.info("MIGRATING database to v17...");
                  sleep(500);
                  LOG.info("Migration done.");
                  return (complete, ops) ->
                      ops.deactivate(DeactivateUpdate.toState(State.RECORD).build());
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource)
            .startTasks(migration)
            .pollingInterval(Duration.ofSeconds(1))
            .registerShutdownHook()
            .build();

    logExistingRecords(scheduler);

    scheduler.start();
    sleep(3000);

    logExistingRecords(scheduler);

    scheduler.stop();
  }

  private void logExistingRecords(Scheduler scheduler) {
    List<ScheduledExecution<Object>> records =
        scheduler.getScheduledExecutions(ScheduledExecutionsFilter.deactivated());
    if (records.isEmpty()) {
      LOG.info("No existing RECORD rows in the DB (fresh run).");
    } else {
      LOG.info("Existing deactivated rows ({}):", records.size());
      for (ScheduledExecution<Object> r : records) {
        LOG.info(
            "  {} state={} lastSuccess={}",
            r.getTaskInstance().getTaskName() + "/" + r.getTaskInstance().getId(),
            r.getState(),
            r.getLastSuccess());
      }
    }
  }
}
