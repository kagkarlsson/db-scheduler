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
package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.serializer.GsonSerializer;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public class GsonSerializerMain extends Example {

    public static void main(String[] args) {
        new GsonSerializerMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        OneTimeTask<JsonData> myAdhocTask = Tasks.oneTime("json-task", JsonData.class)
            .execute((inst, ctx) -> {
                System.out.println("Executed! Custom data: " + inst.getData());
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource, myAdhocTask)
            .serializer(new GsonSerializer())
            .registerShutdownHook()
            .build();

        scheduler.start();

        scheduler.schedule(myAdhocTask.instance("id1", new JsonData(1001L, Instant.now())), Instant.now().plusSeconds(5));
    }

    public static class JsonData {
        public final long id;
        private final Instant time;

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
            return id == jsonData.id &&
                Objects.equals(time, jsonData.time);
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
