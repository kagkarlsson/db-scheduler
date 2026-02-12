package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.boot.config.RecurringTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TaskFromAnnotationWithZoneIdConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(TaskFromAnnotationWithZoneIdConfiguration.class);

  @RecurringTask(
      name = "taskFromAnnotationWithZoneId",
      cron = "0 0 7 19 * *",
      zoneId = "Australia/Tasmania")
  public void taskFromAnnotationWithZoneId() {
    log.info("I'm a task from annotation with zone id");
  }
}
