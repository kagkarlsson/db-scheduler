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

import com.github.kagkarlsson.scheduler.serializer.gson.InstantAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.time.Instant;

public class GsonSerializer implements Serializer {
    private final Gson gson;

    public static Gson getDefaultGson() {
        return new GsonBuilder()
            .serializeNulls()
            .registerTypeAdapter(Instant.class, new InstantAdapter())
            .create();
    }

    public GsonSerializer() {
        this(getDefaultGson());
    }

    public GsonSerializer(Gson gson) {
        this.gson = gson;
    }

    @Override
    public byte[] serialize(Object object) {
        return gson.toJson(object).getBytes();
    }

    @Override
    public <T> T deserialize(Class<T> clazz, byte[] serializedData) {
        return gson.fromJson(new String(serializedData), clazz);
    }

}
