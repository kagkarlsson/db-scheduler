package com.github.kagkarlsson.scheduler.boot.testsupport;

import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class SingleTaskConfiguration {
  private static final Logger log = LoggerFactory.getLogger(SingleTaskConfiguration.class);

  @Bean("singleStringTask")
  public Task<String> singleStringTask() {
    return Tasks.recurring("single-string-task", java.time.Duration.ofMinutes(1))
        .execute((instance, context) -> log.info("Executing single task {}", instance));
  }
}
