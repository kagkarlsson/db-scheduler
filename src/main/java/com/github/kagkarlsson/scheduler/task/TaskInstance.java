/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task;

import java.util.Objects;
import java.util.function.Supplier;

public final class TaskInstance<T> implements TaskInstanceId {

	private final TaskDescriptor<T> taskDescriptor;
	private final String id;
	private final Supplier<T> dataSupplier;
	private final Supplier<byte[]> serializedDataSupplier;

	public TaskInstance(TaskDescriptor<T> taskDescriptor, String id) {
		this(taskDescriptor, id, (T) null);
	}

    public TaskInstance(TaskDescriptor<T> taskDescriptor, String id, T data) {
        this.taskDescriptor = taskDescriptor;
        this.id = id;
        this.dataSupplier = () -> data;
        this.serializedDataSupplier = memoize(() -> this.taskDescriptor.getSerializer().serialize(data));
    }

    public TaskInstance(TaskDescriptor<T> taskDescriptor, String id, byte[] serializedData) {
        this.taskDescriptor = taskDescriptor;
        this.id = id;
        this.serializedDataSupplier = () -> serializedData;
        this.dataSupplier = memoize(() -> this.taskDescriptor.getSerializer().deserialize(serializedData));
    }

	public String getTaskAndInstance() {
		return taskDescriptor.getName() + "_" + id;
	}

	public String getTaskName() {
		return taskDescriptor.getName();
	}

	@Override
	public String getId() {
		return id;
	}

	public T getData() {
		return dataSupplier.get();
	}

	public byte[] getSerializedData() {
		return serializedDataSupplier.get();
	}

	public TaskDescriptor<T> getTaskDescriptor() {
		return taskDescriptor;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TaskInstance that = (TaskInstance) o;
		return Objects.equals(taskDescriptor.getName(), that.taskDescriptor.getName()) &&
				Objects.equals(id, that.id);
	}

	@Override
	public int hashCode() {
		return Objects.hash(taskDescriptor.getName(), id);
	}

	@Override
	public String toString() {
		return "TaskInstance: " +
				"task=" + taskDescriptor.getName() +
				", id=" + id;
	}

    private static <T> Supplier<T> memoize(Supplier<T> original) {
        return new Supplier<T>() {
            Supplier<T> delegate = this::firstTime;
            boolean initialized;
            public T get() {
                return delegate.get();
            }
            private synchronized T firstTime() {
                if(!initialized) {
                    T value = original.get();
                    delegate = () -> value;
                    initialized = true;
                }
                return delegate.get();
            }
        };
    }
}
