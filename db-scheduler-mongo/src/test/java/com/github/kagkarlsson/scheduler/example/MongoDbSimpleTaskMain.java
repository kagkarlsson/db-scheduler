package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.MongoScheduler;
import com.github.kagkarlsson.scheduler.MongoSchedulerBuilder;
import com.github.kagkarlsson.scheduler.MongoTaskRepository;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.utils.TestUtils;
import com.github.kagkarlsson.scheduler.utils.TestUtils.MongoTools;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbSimpleTaskMain {

    private static final Logger LOG = LoggerFactory.getLogger(MongoTaskRepository.class);

    public static void main(String[] args) throws IOException {
        LOG.info("Starting demo app ...");
        MongoTools mongoTools = TestUtils.startEmbeddedMongo();

        // Simple Task
        OneTimeTask<SampleTaskData> oneTimeTask = Tasks.oneTime("one-time", SampleTaskData.class)
            .execute((TaskInstance<SampleTaskData> inst, ExecutionContext ctx) -> {
                LOG.info("Trigger of execution {}, with data {}", ctx.getExecution(), inst);
            });

        // Instantiation of mongodb based scheduler
        List<Task<?>> knownTasks = new ArrayList<>();
        // Register the one time task to scheduler
        knownTasks.add(oneTimeTask);

        SchedulerBuilder builder = MongoScheduler
            .create(mongoTools.getClient(), "scheduler-database", "scheduler-collection",
                knownTasks).pollingInterval(Duration.ofSeconds(5));
        Scheduler scheduler = builder.build();
        // Start scheduler
        scheduler.start();
        // Start one time task
        scheduler.schedule(oneTimeTask.instance("test-instance"),
            Instant.now().plus(5, ChronoUnit.SECONDS));

        Document task = mongoTools.getClient().getDatabase("scheduler-database")
            .getCollection("scheduler-collection").find().first();
        LOG.info("Task found {}", task);

        // Proper shutdown of the scheduler
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal.");
            scheduler.stop();
            mongoTools.getMongodExecutable().stop();
            mongoTools.getMongodProcess().stop();
        }));

    }

    public static class SampleTaskData {

        private String taskContent;

        public String getTaskContent() {
            return taskContent;
        }

        public void setTaskContent(String taskContent) {
            this.taskContent = taskContent;
        }

        @Override
        public String toString() {
            return "SampleTaskData{" +
                "taskContent='" + taskContent + '\'' +
                '}';
        }
    }
}
