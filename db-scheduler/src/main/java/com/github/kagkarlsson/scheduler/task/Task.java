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

import java.time.Instant;

public interface Task<T> extends ExecutionHandler<T>, HasTaskName {
    String getName();

    Class<T> getDataClass();

    TaskInstance<T> instance(String id);
    TaskInstance<T> instance(String id, T data);

    default TaskInstanceId instanceId(String id) {  return TaskInstanceId.of(getName(), id); };

    SchedulableInstance<T> schedulableInstance(String id);
    SchedulableInstance<T> schedulableInstance(String id, T data);

    FailureHandler<T> getFailureHandler();

    DeadExecutionHandler<T> getDeadExecutionHandler();

    @Override
    default String getTaskName() {
        return getName();
    }
}
