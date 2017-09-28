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

import java.io.*;

public abstract class Task<T> implements ExecutionHandler<T> {
	protected final TaskDescriptor<T> descriptor;
	private final CompletionHandler completionHandler;
	private final DeadExecutionHandler deadExecutionHandler;

	public Task(TaskDescriptor<T> descriptor, CompletionHandler completionHandler, DeadExecutionHandler deadExecutionHandler) {
		this.descriptor = descriptor;
		this.completionHandler = completionHandler;
		this.deadExecutionHandler = deadExecutionHandler;
	}

	public String getName() {
		return descriptor.getName();
	}

	public CompletionHandler getCompletionHandler() {
		return completionHandler;
	}

	public DeadExecutionHandler getDeadExecutionHandler() {
		return deadExecutionHandler;
	}

	@Override
	public String toString() {
		return "Task " +
				"task=" + getName();
	}

	public interface Serializer<T> {
		byte[] serialize(T data);
		T deserialize(byte[] serializedData);
        Serializer NO_SERIALIZER = new Serializer<Void>() {
            @Override
            public byte[] serialize(Void data) {
                return new byte[0];
            }

            @Override
            public Void deserialize(byte[] serializedData) {
                return null;
            }
        };
		Serializer JAVA_SERIALIZER = new Serializer<Object>() {
			public byte[] serialize(Object data) {
				if(data == null) return null;
				try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos)) {
					out.writeObject(data);
					return bos.toByteArray();
				} catch(Exception e) {
					throw new RuntimeException("Failed to serialize object", e);
				}
			}
			public Object deserialize(byte[] serializedData) {
				if(serializedData == null) return null;
				try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedData);
					 ObjectInput in = new ObjectInputStream(bis)) {
					return in.readObject();
				} catch(Exception e) {
					throw new RuntimeException("Failed to deserialize object", e);
				}
			}
		};
	}
}

