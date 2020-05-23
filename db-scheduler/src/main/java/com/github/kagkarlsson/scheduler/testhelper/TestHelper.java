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
package com.github.kagkarlsson.scheduler.testhelper;

import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.jdbc.DefaultJdbcCustomization;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

public class TestHelper {

    public static ManualSchedulerBuilder createManualScheduler(DataSource dataSource, Task<?>... knownTasks) {
        return new ManualSchedulerBuilder(dataSource, Arrays.asList(knownTasks));
    }

    public static ManualSchedulerBuilder createManualScheduler(DataSource dataSource, List<Task<?>> knownTasks) {
        return new ManualSchedulerBuilder(dataSource, knownTasks);
    }


    public static class ManualSchedulerBuilder extends SchedulerBuilder {
        private SettableClock clock;

        public ManualSchedulerBuilder(DataSource dataSource, List<Task<?>> knownTasks) {
            super(dataSource, knownTasks);
        }

        public ManualSchedulerBuilder clock(SettableClock clock) {
            this.clock = clock;
            return this;
        }

        public <T extends Task<?> & OnStartup> ManualSchedulerBuilder startTasks(List<T> startTasks) {
            super.startTasks(startTasks);
            return this;
        }

        public ManualSchedulerBuilder statsRegistry(StatsRegistry statsRegistry) {
            super.statsRegistry = statsRegistry;
            return this;
        }

        public ManualScheduler build() {
            final TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, knownTasks);
            final JdbcTaskRepository taskRepository = new JdbcTaskRepository(dataSource, new DefaultJdbcCustomization(), tableName, taskResolver, new SchedulerName.Fixed("manual"), serializer);

            return new ManualScheduler(clock, taskRepository, taskResolver, executorThreads, new DirectExecutorService(), schedulerName, waiter, heartbeatInterval, enableImmediateExecution, statsRegistry, pollingLimit, deleteUnresolvedAfter, startTasks);
        }

        public ManualScheduler start() {
            ManualScheduler scheduler = build();
            scheduler.start();
            return scheduler;
        }
    }

    private static class DirectExecutorService extends AbstractExecutorService {

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return new ArrayList<>();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

}
