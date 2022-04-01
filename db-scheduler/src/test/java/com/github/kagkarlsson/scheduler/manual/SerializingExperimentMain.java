/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.manual;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.serializer.GsonSerializer;
import com.github.kagkarlsson.scheduler.serializer.JavaSerializer;
import com.github.kagkarlsson.scheduler.serializer.MigratingSerializer;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/*

    Setup for testing out changes to code with persistent data.

    docker run -d --name my_postgres -v my_dbdata:/var/lib/postgresql/data -p 54320:5432 -e POSTGRES_PASSWORD=my_password postgres:13
    psql -h localhost -p 54320 postgres postgres
    CREATE TABLE ....

    run scheduler...

    select convert_from(task_data, 'UTF-8') from scheduled_tasks ;
 */
public class SerializingExperimentMain {

    public static void main(String[] args) {
        final PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{ "localhost" });
        ds.setDatabaseName("postgres");
        ds.setUser("postgres");
        ds.setPassword("my_password");
        ds.setPortNumbers(new int[] { 54320});

        new SerializingExperimentMain().run(ds);
    }

    public void run(DataSource dataSource) {

        final RecurringTask<MyData> task = Tasks.recurring("serializing-task", Schedules.fixedDelay(Duration.ofSeconds(1)), MyData.class)
            .executeStateful((inst, ctx) -> {
                System.out.println("Executed! Custom data: " + inst.getData());
                return new MyData(1002L, Instant.now()); // same, just triggering serialization
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource)
            .startTasks(task)
            .pollingInterval(Duration.ofSeconds(1))
//            .serializer(new GsonSerializer())
            .serializer(new MigratingSerializer(new JavaSerializer(), new GsonSerializer()))
            .registerShutdownHook()
            .build();

        scheduler.start();
    }

    public static class MyData {
        public final long id;
        private final Instant time;

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
            return id == myData.id &&
                Objects.equals(time, myData.time);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, time);
        }

        @Override
        public String toString() {
            return "JsonData{" +
                "id=" + id +
                ", time=" + time +
                '}';
        }
    }

}
