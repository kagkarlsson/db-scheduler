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
package com.github.kagkarlsson.scheduler.feature;

import static com.github.kagkarlsson.scheduler.DbUtils.countExecutions;
import static com.github.kagkarlsson.scheduler.TestTasks.ONETIME;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class DeleteOldDeactivatedExecutionsTest {

  private final SettableClock clock = new SettableClock();

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @Test
  public void should_iterate_until_all_old_deactivated_are_removed() {
    var task = Tasks.oneTime(ONETIME).onCompleteKeep(State.COMPLETE).execute(TestTasks.DO_NOTHING);
    var scheduler = createManualScheduler(task);

    for (int i = 0; i < 5; i++) {
      scheduler.schedule(ONETIME.instance("i" + i).scheduledTo(clock.now()));
    }
    scheduler.runAnyDueExecutions();
    assertThat(countExecutions(postgres.getDataSource())).isEqualTo(5);

    // time passes
    clock.tick(Duration.ofDays(15));

    scheduler.runDeleteOldDeactivatedExecutions(2);

    assertThat(countExecutions(postgres.getDataSource())).isZero();
  }

  private ManualScheduler createManualScheduler(OneTimeTask<Void> task) {
    return TestHelper.createManualScheduler(postgres.getDataSource(), task).clock(clock).build();
  }
}
