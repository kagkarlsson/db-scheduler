package com.github.kagkarlsson.scheduler.serializer.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.Instant;

public class InstantDeserializer extends StdDeserializer<Instant> {

    public InstantDeserializer() {
        this(null);
    }

    public InstantDeserializer(Class<Instant> t) {
        super(t);
    }

    @Override
    public Instant deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        return InstantSerializer.FORMATTER.parse(jsonParser.getText(), Instant::from);
    }

}
