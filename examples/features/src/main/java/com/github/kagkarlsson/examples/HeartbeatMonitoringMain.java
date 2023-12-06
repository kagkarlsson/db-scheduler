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

import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.scheduler.HeartbeatState;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;

public class HeartbeatMonitoringMain extends Example {

  public static void main(String[] args) {
    new HeartbeatMonitoringMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {

    OneTimeTask<Void> waitForStaleHeartbeatTask =
        Tasks.oneTime("wait-for-stale-heartbeat-task", Void.class)
            .execute(
                (inst, ctx) -> {
                  System.out.println("Running!");
                  while (ctx.getCurrentlyExecuting().getHeartbeatState().getFractionDead() < 0.7) {
                    sleep(100);
                    printHeartbeat(ctx.getCurrentlyExecuting().getHeartbeatState());
                  }
                  printHeartbeat(ctx.getCurrentlyExecuting().getHeartbeatState());
                  System.out.println("Done!");
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, waitForStaleHeartbeatTask)
            .threads(5)
            .heartbeatInterval(Duration.ofSeconds(1))
            .pollingInterval(Duration.ofSeconds(1))
            .build();

    scheduler.start();

    scheduler.schedule(waitForStaleHeartbeatTask.instance("1045"), Instant.now());

    sleep(2000);
    JdbcRunner jdbcRunner = new JdbcRunner(dataSource);

    // simulate something that will cause heartbeating to fail
    System.out.println("Fake update on execution to cause heartbeat-update to fail.");
    jdbcRunner.execute("update scheduled_tasks set version = version + 1", NOOP);
  }

  private void printHeartbeat(HeartbeatState heartbeatState) {
    System.out.printf(
        "Will keep running until heartbeat-failure detected. Current state: failed-heartbeats=%s, fraction-dead=%s, stale=%s\n",
        heartbeatState.getFailedHeartbeats(),
        heartbeatState.getFractionDead(),
        heartbeatState.hasStaleHeartbeat());
  }
}
