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
package com.github.kagkarlsson.scheduler.serializer.gson;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.time.ZoneId;

public class ZoneIdAdapter extends TypeAdapter<ZoneId> {

  @Override
  public void write(JsonWriter jsonWriter, ZoneId zoneId) throws IOException {
    jsonWriter.value(zoneId.getId());
  }

  @Override
  public ZoneId read(JsonReader jsonReader) throws IOException {
    return ZoneId.of(jsonReader.nextString());
  }
}
