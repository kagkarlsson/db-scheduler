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
package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.github.kagkarlsson.scheduler.boot.autoconfigure.jackson3.InstantDeserializer;
import com.github.kagkarlsson.scheduler.boot.autoconfigure.jackson3.InstantSerializer;
import com.github.kagkarlsson.scheduler.exceptions.SerializationException;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.serializer.jackson.ScheduleMixin;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import java.time.Instant;
import java.util.function.Consumer;
import tools.jackson.core.JacksonException;
import tools.jackson.core.Version;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;

public class Jackson3Serializer implements Serializer {
  private final JsonMapper jsonMapper;

  public Jackson3Serializer() {
    this(getDefaultJsonMapper());
  }

  public Jackson3Serializer(JsonMapper jsonMapper) {
    this.jsonMapper = jsonMapper;
  }

  public Jackson3Serializer(Consumer<JsonMapper> jsonMapperCustomizer) {
    JsonMapper defaultJsonMapper = getDefaultJsonMapper();
    jsonMapperCustomizer.accept(defaultJsonMapper);
    this.jsonMapper = defaultJsonMapper;
  }

  public static JsonMapper getDefaultJsonMapper() {
    SimpleModule module = new SimpleModule("CustomInstantModule", Version.unknownVersion());
    module.addSerializer(Instant.class, new InstantSerializer());
    module.addDeserializer(Instant.class, new InstantDeserializer());

    return JsonMapper.builder()
        .enable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
        .changeDefaultVisibility(v -> v.withFieldVisibility(JsonAutoDetect.Visibility.ANY))
        .addMixIn(Schedule.class, ScheduleMixin.class)
        .addModule(module)
        .build();
  }

  @Override
  public byte[] serialize(Object object) {
    try {
      return jsonMapper.writeValueAsBytes(object);
    } catch (JacksonException e) {
      throw new SerializationException("Failed to serialize object.", e);
    }
  }

  @Override
  public <T> T deserialize(Class<T> clazz, byte[] serializedData) {
    try {
      return jsonMapper.readValue(serializedData, clazz);
    } catch (JacksonException e) {
      throw new SerializationException("Failed to deserialize object.", e);
    }
  }
}
