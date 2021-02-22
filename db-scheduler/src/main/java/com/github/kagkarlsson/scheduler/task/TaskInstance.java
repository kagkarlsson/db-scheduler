/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task;

import java.util.function.Supplier;

public final class TaskInstance<T> implements TaskInstanceId {

    private static final int DEFAULT_PRIORITY = 0;

    private final String taskName;
    private final String id;
    private final Supplier<T> dataSupplier;
    private final int priority;

    public TaskInstance(String taskName, String id) {
        this(taskName, id, (T) null);
    }

    public TaskInstance(String taskName, String id, T data) {
        this(taskName, id, () -> data);
    }

    public TaskInstance(String taskName, String id, Supplier<T> dataSupplier) {
        this(taskName, id, dataSupplier, DEFAULT_PRIORITY);
    }

    public TaskInstance(String taskName, String id, Supplier<T> dataSupplier, int priority) {
        this.taskName = taskName;
        this.id = id;
        this.dataSupplier = dataSupplier;
        this.priority = priority;
    }

    public String getTaskAndInstance() {
        return taskName + "_" + id;
    }

    public String getTaskName() {
        return taskName;
    }

    @Override
    public String getId() {
        return id;
    }

    public T getData() {
        return dataSupplier.get();
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TaskInstance<?> that = (TaskInstance<?>) o;

        if (!taskName.equals(that.taskName)) {
            return false;
        }
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        int result = taskName.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TaskInstance: " +
                "task=" + taskName +
                ", id=" + id +
                ", priority=" + priority;
    }

    public static class Builder<T> {

        private final String taskName;
        private final String id;
        private Supplier<T> dataSupplier = () -> (T) null;
        private int priority = DEFAULT_PRIORITY;

        public Builder(String taskName, String id) {
            this.id = id;
            this.taskName = taskName;
        }

        public Builder<T> setDataSupplier(Supplier<T> dataSupplier) {
            this.dataSupplier = dataSupplier;
            return this;
        }

        public Builder<T> setData(T data) {
            this.dataSupplier = () -> (T) data;
            ;
            return this;
        }

        public Builder<T> setPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public TaskInstance<T> build() {
            return new TaskInstance<>(taskName, id, dataSupplier, priority);
        }
    }
}
