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
package com.github.kagkarlsson.scheduler.serializer;

import com.github.kagkarlsson.scheduler.serializer.gson.*;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.Daily;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.function.Consumer;

public class GsonSerializer implements Serializer {
  public static final Charset CHARSET = StandardCharsets.UTF_8;
  private final Gson gson;

  public static GsonBuilder getDefaultGson() {
    RuntimeTypeAdapterFactory<Schedule> runtimeTypeAdapterFactory =
        RuntimeTypeAdapterFactory.of(Schedule.class, "type")
            .registerSubtype(CronSchedule.class, "cron")
            .registerSubtype(FixedDelay.class, "fixedDelay")
            .registerSubtype(Daily.class, "daily");

    return new GsonBuilder()
        .serializeNulls()
        .registerTypeAdapter(Instant.class, new InstantAdapter())
        .registerTypeAdapter(Duration.class, new DurationAdapter())
        .registerTypeAdapter(LocalTime.class, new LocalTimeAdapter())
        .registerTypeHierarchyAdapter(ZoneId.class, new ZoneIdAdapter())
        .registerTypeAdapterFactory(runtimeTypeAdapterFactory);
  }

  public GsonSerializer() {
    this(getDefaultGson().create());
  }

  public GsonSerializer(Gson gson) {
    this.gson = gson;
  }

  public GsonSerializer(Consumer<GsonBuilder> gsonCustomizer) {
    GsonBuilder defaultGson = getDefaultGson();
    gsonCustomizer.accept(defaultGson);
    this.gson = defaultGson.create();
  }

  @Override
  public byte[] serialize(Object object) {
    return gson.toJson(object).getBytes(CHARSET);
  }

  @Override
  public <T> T deserialize(Class<T> clazz, byte[] serializedData) {
    return gson.fromJson(new String(serializedData, CHARSET), clazz);
  }
}
