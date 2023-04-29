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
package com.github.kagkarlsson.scheduler.serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializerWithFallbackDeserializers implements Serializer {

  private static final Logger LOG =
      LoggerFactory.getLogger(SerializerWithFallbackDeserializers.class);
  private final Serializer serializer;
  private final Serializer fallbackDeserializer;

  public SerializerWithFallbackDeserializers(
      Serializer serializer, Serializer fallbackDeserializer) {
    this.serializer = serializer;
    this.fallbackDeserializer = fallbackDeserializer;
  }

  @Override
  public byte[] serialize(Object data) {
    return serializer.serialize(data);
  }

  @Override
  public <T> T deserialize(Class<T> clazz, byte[] serializedData) {
    try {
      return serializer.deserialize(clazz, serializedData);
    } catch (Exception e) {
      LOG.debug("Failed to deserialize data. Trying fallback Serializer.", e);
      return fallbackDeserializer.deserialize(clazz, serializedData);
    }
  }
}
