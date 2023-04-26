/**
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
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.io.Serializable;
import java.time.Instant;
import javax.sql.DataSource;

public class OneTimeTaskMain extends Example {

  public static void main(String[] args) {
    new OneTimeTaskMain().runWithDatasource();
  }

  @Override
  public void run(DataSource dataSource) {

    OneTimeTask<MyTaskData> myAdhocTask =
        Tasks.oneTime("my-typed-adhoc-task", MyTaskData.class)
            .execute(
                (inst, ctx) -> {
                  System.out.println("Executed! Custom data, Id: " + inst.getData().id);
                });

    final Scheduler scheduler = Scheduler.create(dataSource, myAdhocTask).threads(5).build();

    scheduler.start();

    // Schedule the task for execution a certain time in the future and optionally provide custom
    // data for the execution
    scheduler.schedule(
        myAdhocTask.instance("1045", new MyTaskData(1001L)), Instant.now().plusSeconds(5));
  }

  public static class MyTaskData implements Serializable {
    public final long id;

    public MyTaskData(long id) {
      this.id = id;
    }
  }
}
