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

public final class TaskInstance<T> {

	private final Task task;
	private final String id;
	private Supplier<T> dataSupplier;

	public TaskInstance(Task task, String id) {
		this(task, id, (T) null);
	}

    public TaskInstance(Task task, String id, T data) {
        this(task, id, () -> data);
    }

	public TaskInstance(Task task, String id, Supplier<T> dataSupplier) {
		this.task = task;
		this.id = id;
		this.dataSupplier = memoize(dataSupplier);
	}

	public String getTaskAndInstance() {
		return task.getName() + "_" + id;
	}

	public Task getTask() {
		return task;
	}

	public String getTaskName() {
		return task.getName();
	}

	public String getId() {
		return id;
	}

	public T getData() {
		return dataSupplier.get();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TaskInstance that = (TaskInstance) o;
		return Objects.equals(task.getName(), that.task.getName()) &&
				Objects.equals(id, that.id);
	}

	@Override
	public int hashCode() {
		return Objects.hash(task.getName(), id);
	}

	@Override
	public String toString() {
		return "TaskInstance: " +
				"task=" + task.getName() +
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
