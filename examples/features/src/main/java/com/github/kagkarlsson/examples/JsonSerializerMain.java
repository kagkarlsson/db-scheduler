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
import com.github.kagkarlsson.scheduler.serializer.GsonSerializer;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import javax.sql.DataSource;

public class JsonSerializerMain extends Example {

  public static void main(String[] args) {
    new JsonSerializerMain().runWithDatasource();
  }

  public static final TaskDescriptor<JsonData> JSON_TASK = TaskDescriptor.of("json-task", JsonData.class);

  @Override
  public void run(DataSource dataSource) {

    OneTimeTask<JsonData> myAdhocTask =
        Tasks.oneTime(JSON_TASK)
            .execute(
                (inst, ctx) -> {
                  System.out.println("Executed! Custom data: " + inst.getData());
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource, myAdhocTask)
            .serializer(new GsonSerializer()) // also try .serializer(new JacksonSerializer())
            .registerShutdownHook()
            .pollingInterval(Duration.ofSeconds(1))
            .build();

    scheduler.start();

    scheduler.schedule(
        JSON_TASK
            .instance("id1")
            .data(new JsonData(1001L, Instant.now()))
            .scheduledTo(Instant.now().plusSeconds(1)));
  }

  public static class JsonData {
    public long id;
    private Instant time;

    private JsonData() {}

    public JsonData(long id, Instant time) {
      this.id = id;
      this.time = time;
    }

    public Instant getTime() {
      return time;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JsonData jsonData = (JsonData) o;
      return id == jsonData.id && Objects.equals(time, jsonData.time);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, time);
    }

    @Override
    public String toString() {
      return "JsonData{" + "id=" + id + ", time=" + time + '}';
    }
  }
}
