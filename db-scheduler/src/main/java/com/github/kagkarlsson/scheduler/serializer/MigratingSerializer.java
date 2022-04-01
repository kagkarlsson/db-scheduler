package com.github.kagkarlsson.scheduler.serializer;

public class MigratingSerializer implements Serializer {

    private final Serializer serializer;
    private final Serializer[] candidates;

    public MigratingSerializer(Serializer serializer, Serializer ... candidates) {
        this.serializer = serializer;
        this.candidates = candidates;
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
            // TODO
            return candidates[0].deserialize(clazz, serializedData);
        }
    }
}
