package com.github.kagkarlsson.scheduler.task;

public class TaskDescriptor<T> {

    private final String name;
    private final Task.Serializer<T> serializer;

    public TaskDescriptor(String name) {
        this(name, Task.Serializer.NO_SERIALIZER);
    }

    public TaskDescriptor(String name, Task.Serializer<T> serializer) {
        this.name = name;
        this.serializer = serializer;
    }

    public TaskInstance<T> instance(String id) {
        return new TaskInstance<>(this, id);
    }

    public TaskInstance<T> instance(String id, T data) {
        return new TaskInstance<>(this, id, data);
    }

    public String getName() {
        return name;
    }

    public Task.Serializer<T> getSerializer() {
        return serializer;
    }
}
