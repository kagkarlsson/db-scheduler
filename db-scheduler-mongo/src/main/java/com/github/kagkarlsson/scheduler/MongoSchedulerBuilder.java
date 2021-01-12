package com.github.kagkarlsson.scheduler;

import static com.github.kagkarlsson.scheduler.ExecutorUtils.defaultThreadFactoryWithPrefix;
import static com.github.kagkarlsson.scheduler.Scheduler.THREAD_PREFIX;

import com.github.kagkarlsson.scheduler.task.Task;
import com.mongodb.MongoClient;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSchedulerBuilder extends SchedulerBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(MongoSchedulerBuilder.class);

    private final MongoClient mongoClient;

    private final String databaseName;

    /**
     * Scheduler builder for mongo database
     *
     * Example using the database 'scheduler-database' and the collection 'scheduler-collection' :
     * SchedulerBuilder builder = new MongoSchedulerBuilder(mongoTools.getClient(),
     *             "scheduler-database", knownTasks).tableName("scheduler-collection")
     *
     * @param mongoClient - object handling mongo connection
     * @param databaseName - mongo database name
     * @param knownTasks - list of known tasks
     */
    protected MongoSchedulerBuilder(MongoClient mongoClient, String databaseName, String collection,
        List<Task<?>> knownTasks) {
        super(null, knownTasks);
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
        this.tableName = collection;
    }

    @Override
    public Scheduler build() {
        if (pollingLimit < executorThreads) {
            LOG.warn("Polling-limit is less than number of threads. Should be equal or higher.");
        }

        if (schedulerName == null) {
            schedulerName = new SchedulerName.Hostname();
        }

        final TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, knownTasks);

        final MongoTaskRepository mongoTaskRepository = new MongoTaskRepository(taskResolver,
            schedulerName, serializer, databaseName, tableName, mongoClient);
        final MongoTaskRepository clientTaskRepository = new MongoTaskRepository(taskResolver,
            schedulerName, serializer, databaseName, tableName, mongoClient);

        ExecutorService candidateExecutorService = executorService;
        if (candidateExecutorService == null) {
            candidateExecutorService = Executors
                .newFixedThreadPool(executorThreads, defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-"));
        }

        LOG.info("Creating scheduler with configuration: threads={}, pollInterval={}s, heartbeat={}s enable-immediate-execution={}, table-name={}, name={}",
            executorThreads,
            waiter.getWaitDuration().getSeconds(),
            heartbeatInterval.getSeconds(),
            enableImmediateExecution,
            tableName,
            schedulerName.getName());

        return new MongoScheduler(clock, mongoTaskRepository, clientTaskRepository, taskResolver, executorThreads, candidateExecutorService,
            schedulerName, waiter, heartbeatInterval, enableImmediateExecution, statsRegistry, pollingLimit,
            deleteUnresolvedAfter, shutdownMaxWait, startTasks);
    }
}
