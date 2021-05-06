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
package com.github.kagkarlsson.scheduler.task.helper;

import com.github.kagkarlsson.scheduler.Clock;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;

import java.time.Instant;
import java.util.function.Function;

class ScheduleOnceOnStartup<T> implements ScheduleOnStartup<T> {
    private String instance;
    private T data;
    private Function<Instant, Instant> firstExecutionTime;

    ScheduleOnceOnStartup(String instance) {
        this(instance, null);
    }

    ScheduleOnceOnStartup(String instance, T data) {
        this(instance, data, Function.identity());
    }

    ScheduleOnceOnStartup(String instance, T data, Function<Instant, Instant> firstExecutionTime) {
        this.firstExecutionTime = firstExecutionTime;
        this.instance = instance;
        this.data = data;
    }

    public void apply(SchedulerClient scheduler, Clock clock, Task<T> task) {
        scheduler.schedule(getSchedulableInstance(task), firstExecutionTime.apply(clock.now()));
    }

    private TaskInstance<T> getSchedulableInstance(Task<T> task) {
        if (data == null) {
            return task.instance(instance);
        } else {
            return task.instance(instance, data);
        }
    }
}
