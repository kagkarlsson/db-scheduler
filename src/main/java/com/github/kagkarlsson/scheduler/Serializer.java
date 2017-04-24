package com.github.kagkarlsson.scheduler;

public interface Serializer<T> {
    byte[] serialize(T data);
    T deserialize(byte[] serializedData);
}
