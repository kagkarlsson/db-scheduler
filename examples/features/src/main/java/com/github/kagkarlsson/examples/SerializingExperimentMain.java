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

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.serializer.GsonSerializer;
import com.github.kagkarlsson.scheduler.serializer.JavaSerializer;
import com.github.kagkarlsson.scheduler.serializer.SerializerWithFallbackDeserializers;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

/*

   Setup for testing out changes to code with persistent data. Requires non-embedded postgres database. Setup using Docker.

   docker run -d --name my_postgres -v my_dbdata:/var/lib/postgresql/data -p 54320:5432 -e POSTGRES_PASSWORD=my_password postgres:13
   psql -h localhost -p 54320 postgres postgres
   create table scheduled_tasks (  task_name text not null,  task_instance text not null,  task_data bytea,  execution_time timestamp with time zone not null,  picked BOOLEAN not null,  picked_by text,  last_success timestamp with time zone,  last_failure timestamp with time zone,  consecutive_failures INT,  last_heartbeat timestamp with time zone,  version BIGINT not null,  priority INT,  PRIMARY KEY (task_name, task_instance));

   get json data
     select convert_from(task_data, 'UTF-8') from scheduled_tasks ;

   Not setting 'serialVersionUID' to begin with might cause this on class-changes:
   java.io.InvalidClassException: com.github.kagkarlsson.examples.SerializingExperimentMain$MyData; local class incompatible: stream classdesc serialVersionUID = 6236001007065622457, local class serialVersionUID = 1

*/
public class SerializingExperimentMain {

  public static void main(String[] args) {
    final PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {"localhost"});
    ds.setDatabaseName("postgres");
    ds.setUser("postgres");
    ds.setPassword("my_password");
    ds.setPortNumbers(new int[] {54320});

    new SerializingExperimentMain().run(ds);
  }

  public void run(DataSource dataSource) {

    final RecurringTask<MyData> task =
        Tasks.recurring(
                "serializing-task", Schedules.fixedDelay(Duration.ofSeconds(1)), MyData.class)
            .executeStateful(
                (inst, ctx) -> {
                  System.out.println("Executed! Custom data: " + inst.getData());
                  return new MyData(1002L, Instant.now()); // same, just triggering serialization
                });

    final Scheduler scheduler =
        Scheduler.create(dataSource)
            .startTasks(task)
            .pollingInterval(Duration.ofSeconds(1))
            .heartbeatInterval(Duration.ofSeconds(3))
            //            .serializer(new GsonSerializer())
            //            .serializer(new JavaSerializer())
            //            .serializer(new SerializerWithFallbackDeserializers(new GsonSerializer(),
            // new JavaSerializer()))
            .serializer(
                new SerializerWithFallbackDeserializers(new JavaSerializer(), new GsonSerializer()))
            .registerShutdownHook()
            .build();

    scheduler.start();
  }

  public static class MyData implements Serializable {
    public final long id;
    private final Instant time;
    private static final long serialVersionUID = 6236001007065622457L;

    public MyData(long id, Instant time) {
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
      MyData myData = (MyData) o;
      return id == myData.id && Objects.equals(time, myData.time);
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
