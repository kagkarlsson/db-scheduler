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
package com.github.kagkarlsson.scheduler;

import java.io.*;

public interface Serializer {

    byte[] serialize(Object data);

    <T> T deserialize(Class<T> clazz, byte[] serializedData);

    Serializer DEFAULT_JAVA_SERIALIZER = new Serializer() {

        public byte[] serialize(Object data) {
            if (data == null)
                return null;
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutput out = new ObjectOutputStream(bos)) {
                out.writeObject(data);
                return bos.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize object", e);
            }
        }

        public <T> T deserialize(Class<T> clazz, byte[] serializedData) {
            if (serializedData == null)
                return null;
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedData);
                 ObjectInput in = new ObjectInputStream(bis)) {
                return clazz.cast(in.readObject());
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize object", e);
            }
        }
    };
}
