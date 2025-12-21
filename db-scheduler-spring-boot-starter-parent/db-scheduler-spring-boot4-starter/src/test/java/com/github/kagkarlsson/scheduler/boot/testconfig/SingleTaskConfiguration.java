package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class SingleTaskConfiguration {
  private static final Logger log = LoggerFactory.getLogger(SingleTaskConfiguration.class);

  @Bean("singleStringTask")
  public Task<Void> singleStringTask() {
    return Tasks.recurring("single-string-task", FixedDelay.of(Duration.ofMinutes(1)))
        .execute((instance, context) -> log.info("Executing single task {}", instance));
  }
}
