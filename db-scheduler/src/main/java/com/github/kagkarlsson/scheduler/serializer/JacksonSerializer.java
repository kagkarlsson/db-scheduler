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
package com.github.kagkarlsson.scheduler.serializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.github.kagkarlsson.scheduler.exceptions.SerializationException;
import com.github.kagkarlsson.scheduler.serializer.gson.InstantAdapter;
import com.github.kagkarlsson.scheduler.serializer.jackson.InstantDeserializer;
import com.github.kagkarlsson.scheduler.serializer.jackson.InstantSerializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.sql.Time;
import java.time.Instant;
import java.util.function.Consumer;

public class JacksonSerializer implements Serializer {
    private final ObjectMapper objectMapper;

    public static ObjectMapper getDefaultObjectMapper() {
        SimpleModule module = new SimpleModule();
        module.addSerializer(Instant.class, new InstantSerializer());
        module.addDeserializer(Instant.class, new InstantDeserializer());

        return new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .registerModule(module);
    }

    public JacksonSerializer() {
        this(getDefaultObjectMapper());
    }

    public JacksonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JacksonSerializer(Consumer<ObjectMapper> objectMapperCustomizer) {
        ObjectMapper defaultObjectMapper = getDefaultObjectMapper();
        objectMapperCustomizer.accept(defaultObjectMapper);
        this.objectMapper = defaultObjectMapper;
    }

    @Override
    public byte[] serialize(Object object) {
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize object.", e);
        }
    }

    @Override
    public <T> T deserialize(Class<T> clazz, byte[] serializedData) {
        try {
            return objectMapper.readValue(serializedData, clazz);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize object.", e);
        }
    }

}
