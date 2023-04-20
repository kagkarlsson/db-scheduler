/**
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.serializer.gson;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;

public class LocalTimeAdapter extends TypeAdapter<LocalTime> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofLocalizedTime(FormatStyle.SHORT);

    @Override
    public void write(JsonWriter jsonWriter, LocalTime localTime) throws IOException {
        jsonWriter.value(localTime.format(FORMATTER));
    }

    @Override
    public LocalTime read(JsonReader jsonReader) throws IOException {
        return FORMATTER.parse(jsonReader.nextString(), LocalTime::from);
    }
}
