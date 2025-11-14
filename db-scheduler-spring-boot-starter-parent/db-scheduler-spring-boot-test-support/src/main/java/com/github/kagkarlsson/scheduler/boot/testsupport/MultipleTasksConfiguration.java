package com.github.kagkarlsson.scheduler.boot.testsupport;

import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MultipleTasksConfiguration {
  private static final Logger log = LoggerFactory.getLogger(MultipleTasksConfiguration.class);

  @Bean("firstTask")
  public Task<String> firstTask() {
    return named("first-task");
  }

  @Bean("secondTask")
  public Task<String> secondTask() {
    return named("second-task");
  }

  @Bean("thirdTask")
  public Task<String> thirdTask() {
    return named("third-task");
  }

  private Task<String> named(String name) {
    return Tasks.recurring(name, Duration.ofMinutes(1))
        .execute((instance, context) -> log.info("Executing {}", instance.getTaskName()));
  }
}
