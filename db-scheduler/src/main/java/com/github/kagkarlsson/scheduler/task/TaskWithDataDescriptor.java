/**
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task;

/** Experimental */
public class TaskWithDataDescriptor<T> implements TaskDescriptor<T> {

    private final String taskName;
    private final Class<T> dataClass;

    public TaskWithDataDescriptor(String taskName, Class<T> dataClass) { // TODO: not used?
        this.taskName = taskName;
        this.dataClass = dataClass;
    }

    public TaskInstance<T> instance(String id, T data) {
        return new TaskInstance<>(taskName, id, data);
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public Class<T> getDataClass() {
        return dataClass;
    }

    public TaskInstanceId instanceId(String id) {
        return TaskInstanceId.of(taskName, id);
    }
}
