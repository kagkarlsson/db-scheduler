package com.github.kagkarlsson.scheduler.serializer.jackson;

import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class JacksonSerializerTest {

    @Test
    public void serialize_instant() {
        JacksonSerializer serializer = new JacksonSerializer();

        final Instant now = Instant.now();
        assertEquals(now, serializer.deserialize(Instant.class, serializer.serialize(now)));
    }

}
