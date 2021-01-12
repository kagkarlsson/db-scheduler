package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSchedulerTest {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSchedulerTest.class);

    @RegisterExtension
    public static EmebddedMongodbExtension mongo = new EmebddedMongodbExtension();

    @Test
    void schedulerOneTimeTest() {
        SchedulerBuilder builder = MongoScheduler
            .create(mongo.getMongoClient(), "database", "collection");

        AtomicInteger count = new AtomicInteger();

        OneTimeTask<Object> task = Tasks
            .oneTime("task", Object.class).execute((TaskInstance<Object> inst,
                ExecutionContext ctx) -> {
                LOG.info("Execution");
                count.addAndGet(1);
            });

        builder.knownTasks.add(task);
        builder.executorService(MoreExecutors.newDirectExecutorService());

        Scheduler scheduler = builder.build();
        scheduler.schedule(task.instance("instance-1"), Instant.now());

        scheduler.executeDue();

        Assertions.assertThat(count).hasValue(1);
    }
}
