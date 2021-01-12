package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;
import com.mongodb.MongoClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class MongoScheduler extends Scheduler {

    protected MongoScheduler(Clock clock,
        TaskRepository schedulerTaskRepository,
        TaskRepository clientTaskRepository, TaskResolver taskResolver, int threadpoolSize,
        ExecutorService executorService,
        SchedulerName schedulerName, Waiter executeDueWaiter, Duration heartbeatInterval,
        boolean enableImmediateExecution,
        StatsRegistry statsRegistry, int pollingLimit,
        Duration deleteUnresolvedAfter, Duration shutdownMaxWait,
        List<OnStartup> onStartup) {
        super(clock, schedulerTaskRepository, clientTaskRepository, taskResolver, threadpoolSize,
            executorService, schedulerName, executeDueWaiter, heartbeatInterval,
            enableImmediateExecution,
            statsRegistry, pollingLimit, deleteUnresolvedAfter, shutdownMaxWait, onStartup);
    }

    public static MongoSchedulerBuilder create(MongoClient mongoClient, String database,
        String collection, Task<?>... knownTasks) {
        List<Task<?>> knownTasksList = new ArrayList<>();
        knownTasksList.addAll(Arrays.asList(knownTasks));
        return create(mongoClient, database, collection, knownTasksList);
    }

    public static MongoSchedulerBuilder create(MongoClient mongoClient, String database,
        String collection, List<Task<?>> knownTasks) {
        return new MongoSchedulerBuilder(mongoClient, database, collection, knownTasks);
    }
}
