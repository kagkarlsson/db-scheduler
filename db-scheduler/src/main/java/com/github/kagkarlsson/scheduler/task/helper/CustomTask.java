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
package com.github.kagkarlsson.scheduler.task.helper;

import com.github.kagkarlsson.scheduler.Clock;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.AbstractTask;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.NextExecutionTime;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.SchedulableTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.time.Instant;
import java.util.function.Function;

public abstract class CustomTask<T> extends AbstractTask<T> implements OnStartup {
    private ScheduleOnStartup<T> scheduleOnStartup;
    private final NextExecutionTime defaultExecutionTime;

    public CustomTask(
            String name,
            Class<T> dataClass,
            ScheduleOnStartup<T> scheduleOnStartup,
            Function<Instant, Instant> defaultExecutionTime,
            FailureHandler<T> failureHandler,
            DeadExecutionHandler<T> deadExecutionHandler) {
        super(name, dataClass, failureHandler, deadExecutionHandler);
        this.scheduleOnStartup = scheduleOnStartup;
        this.defaultExecutionTime = NextExecutionTime.from(defaultExecutionTime);
    }

    @Override
    public SchedulableInstance<T> schedulableInstance(String id) {
        return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id), defaultExecutionTime);
    }

    @Override
    public SchedulableInstance<T> schedulableInstance(String id, T data) {
        return new SchedulableTaskInstance<>(new TaskInstance<>(getName(), id, data), defaultExecutionTime);
    }

    @Override
    public void onStartup(SchedulerClient scheduler, Clock clock) {
        if (scheduleOnStartup != null) {
            scheduleOnStartup.apply(scheduler, clock, this);
        }
    }

    @Override
    public String toString() {
        return "CustomTask name=" + getName();
    }
}
