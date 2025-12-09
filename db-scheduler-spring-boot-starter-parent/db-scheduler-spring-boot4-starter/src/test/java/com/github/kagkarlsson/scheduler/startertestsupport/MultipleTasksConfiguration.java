package com.github.kagkarlsson.scheduler.startertestsupport;

import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MultipleTasksConfiguration {
  private static final Logger log = LoggerFactory.getLogger(MultipleTasksConfiguration.class);

  @Bean("firstTask")
  public Task<Void> firstTask() {
    return named("first-task");
  }

  @Bean("secondTask")
  public Task<Void> secondTask() {
    return named("second-task");
  }

  @Bean("thirdTask")
  public Task<Void> thirdTask() {
    return named("third-task");
  }

  private Task<Void> named(String name) {
    return Tasks.recurring(name, FixedDelay.of(Duration.ofMinutes(1)))
        .execute((instance, context) -> log.info("Executing {}", instance.getTaskName()));
  }
}
