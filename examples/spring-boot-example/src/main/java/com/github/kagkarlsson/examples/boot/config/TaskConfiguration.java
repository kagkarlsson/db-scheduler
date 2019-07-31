package com.github.kagkarlsson.examples.boot.config;

import static com.github.kagkarlsson.scheduler.task.schedule.Schedules.fixedDelay;

import com.github.kagkarlsson.examples.boot.CounterService;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TaskConfiguration {
    private static final Logger log = LoggerFactory.getLogger(TaskConfiguration.class);

    /**
     * Define a recurring task with a dependency, which will automatically be picked up by the
     * Spring Boot autoconfiguration.
     */
    @Bean
    Task<Void> recurringSampleTask(CounterService counter) {
        return Tasks
            .recurring("recurring-sample-task", fixedDelay(Duration.ofMinutes(1)))
            .execute((instance, ctx) -> {
                log.info("Running recurring-simple-task. Instance: {}, ctx: {}", instance, ctx);
                counter.increase();
            });
    }

    /**
     * Define a one-time task which have to be manually scheduled.
     */
    @Bean
    Task<Void> sampleOneTimeTask() {
        return Tasks.oneTime("sample-one-time-task")
            .execute((instance, ctx) -> {
                log.info("I am a one-time task!");
            });
    }
}
