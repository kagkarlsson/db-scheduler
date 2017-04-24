package com.github.kagkarlsson.scheduler.task;

import java.io.*;

public abstract class OneTimeTaskWithData<T> extends OneTimeTask {

    private final Serializer<T> serializer;

    public OneTimeTaskWithData(String name, Serializer<T> serializer) {
        super(name);
        this.serializer = serializer;
    }

    public TaskInstance instance(String id, T data) {
        byte[] serialized;
        try {
            serialized = serializer.serialize(data);
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to serialize data for task=" + super.getName() + ", id=" + id, e);
        }

        return super.instance(id, serialized);
    }

    @Override
    public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
        T data;
        try {
            data = serializer.deserialize(taskInstance.getData());
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to deserialize data for " + taskInstance, e);
        }
        execute(taskInstance, data, executionContext);
    }

    public abstract void execute(TaskInstance taskInstance, T taskData, ExecutionContext executionContext);

    public interface Serializer<T> {
        byte[] serialize(T data);
        T deserialize(byte[] data);
    }

    public static class JavaSerializer<T extends Serializable> implements Serializer<T> {

        @Override
        public byte[] serialize(T data) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(os)) {
                oos.writeObject(data);
                return os.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize object", e);
            }
        }

        @Override
        public T deserialize(byte[] data) {
            ByteArrayInputStream is = new ByteArrayInputStream(data);
            try (ObjectInputStream ois = new ObjectInputStream(is)) {
                return (T)ois.readObject();
            } catch (ClassNotFoundException | IOException e) {
                throw new RuntimeException("Failed to deserialize data", e);
            }
        }
    }

}
