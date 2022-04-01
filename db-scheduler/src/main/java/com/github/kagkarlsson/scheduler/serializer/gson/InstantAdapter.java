package com.github.kagkarlsson.scheduler.serializer.gson;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class InstantAdapter extends TypeAdapter<Instant> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT;

    @Override
    public void write(JsonWriter jsonWriter, Instant instant) throws IOException {
        jsonWriter.value(FORMATTER.format(instant));
    }

    @Override
    public Instant read(JsonReader jsonReader) throws IOException {
        return FORMATTER.parse(jsonReader.nextString(), Instant::from);
    }
}
