package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.boot.config.RecurringTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TaskFromAnnotationWithCronPropertyConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(TaskFromAnnotationWithCronPropertyConfiguration.class);

  @RecurringTask(name = "taskFromAnnotationWithCronProperty", cron = "${my-custom-property.cron}")
  public void taskFromAnnotationWithCronProperty() {
    log.info("I'm a task from annotation with property");
  }
}
