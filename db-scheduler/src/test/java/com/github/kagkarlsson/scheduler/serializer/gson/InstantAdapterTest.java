package com.github.kagkarlsson.scheduler.serializer.gson;

import com.github.kagkarlsson.scheduler.serializer.GsonSerializer;
import com.google.gson.Gson;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class InstantAdapterTest {

    @Test
    public void serialize_instant() {
        final Instant now = Instant.now();
        final Gson gson = GsonSerializer.getDefaultGson().create();
        assertEquals(now, gson.fromJson(gson.toJson(now), Instant.class));
    }

    @Test
    public void serialize_duration() {
        final Duration duration = Duration.ofSeconds(10);
        final Gson gson = GsonSerializer.getDefaultGson().create();
        assertEquals(duration, gson.fromJson(gson.toJson(duration), Duration.class));
    }
}
