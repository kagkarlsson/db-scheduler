package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import utils.EventLogger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import utils.Utils;

import java.time.Instant;
import java.util.Random;

import static com.github.kagkarlsson.examples.boot.config.TaskNames.*;

@Configuration
public class ParallellJobConfiguration {

    private TransactionTemplate tt;

    public ParallellJobConfiguration(TransactionTemplate tt) {
        this.tt = tt;
    }

    @Bean
    public Task<Void> parallelJobSpawner() {
        return Tasks.recurring(PARALLEL_JOB_SPAWNER, Schedules.cron("0 0 * * * *"))
            .execute((TaskInstance<Void> taskInstance, ExecutionContext executionContext) -> {

                // Create all or none. SchedulerClient is transactions-aware since a Spring datasource is used
                tt.executeWithoutResult((TransactionStatus status) -> {
                    for (int quarter = 1; quarter < 5; quarter++) {
                        // can use 'executionContext.getSchedulerClient()' to avoid circular dependency
                        executionContext.getSchedulerClient().schedule(PARALLEL_JOB.instance("q"+quarter, quarter), Instant.now());
                    }
                });
                EventLogger.logTask(PARALLEL_JOB_SPAWNER, "Ran. Scheduled tasks for generating quarterly report.");
            });
    }

    @Bean
    public Task<Integer> parallelJob() {
        return Tasks.oneTime(PARALLEL_JOB)
            .execute((TaskInstance<Integer> taskInstance, ExecutionContext executionContext) -> {
                long startTime = System.currentTimeMillis();

                Utils.sleep(new Random().nextInt(10) *1000);

                String threadName = Thread.currentThread().getName();
                EventLogger.logTask(PARALLEL_JOB, String.format("Ran. Generated report for quarter Q%s  (in thread '%s', duration %sms)", taskInstance.getData(), threadName, System.currentTimeMillis() - startTime));
            });
    }

}
