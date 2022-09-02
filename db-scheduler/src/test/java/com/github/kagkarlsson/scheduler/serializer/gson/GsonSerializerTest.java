package com.github.kagkarlsson.scheduler.serializer.gson;

import com.github.kagkarlsson.scheduler.serializer.GsonSerializer;
import com.google.gson.Gson;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class GsonSerializerTest {

    @Test
    public void serialize_instant() {
        final GsonSerializer gsonSerializer = new GsonSerializer();
        final Instant now = Instant.now();

        assertEquals(now, gsonSerializer.deserialize(Instant.class, gsonSerializer.serialize(now)));
    }

}
