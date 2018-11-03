package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.task.Task;
import com.google.common.util.concurrent.MoreExecutors;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;

public class TestHelper {

    public static InlinedSchedulerBuilder createInlinedScheduler(DataSource dataSource, Task<?> ... knownTasks) {
        return new InlinedSchedulerBuilder(dataSource, Arrays.asList(knownTasks));
    }

    public static InlinedSchedulerBuilder createInlinedScheduler(DataSource dataSource, List<Task<?>> knownTasks) {
        return new InlinedSchedulerBuilder(dataSource, knownTasks);
    }


    public static class InlinedSchedulerBuilder extends Scheduler.Builder {
        private SettableClock clock;

        public InlinedSchedulerBuilder(DataSource dataSource, List<Task<?>> knownTasks) {
            super(dataSource, knownTasks);
        }

        public InlinedSchedulerBuilder clock(SettableClock clock) {
            this.clock = clock;
            return this;
        }

        public InlinedScheduler build() {
            final TaskResolver taskResolver = new TaskResolver(knownTasks);
            final JdbcTaskRepository taskRepository = new JdbcTaskRepository(dataSource, tableName, taskResolver, schedulerName, serializer);

            return new InlinedScheduler(clock, taskRepository, taskResolver, executorThreads, MoreExecutors.newDirectExecutorService(), schedulerName, waiter, heartbeatInterval, enableImmediateExecution, statsRegistry, startTasks);
        }

        public InlinedScheduler start() {
            InlinedScheduler scheduler = build();
            scheduler.start();
            return scheduler;
        }


    }

}
