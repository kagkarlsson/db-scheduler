package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.boot.config.RecurringTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TaskFromAnnotationWithCronConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(TaskFromAnnotationWithCronConfiguration.class);

  @RecurringTask(name = "taskFromAnnotationWithCron", cron = "0 0 7 19 * *")
  public void taskFromAnnotationWithCron() {
    log.info("I'm a task from annotation");
  }
}
