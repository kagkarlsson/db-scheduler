package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.boot.config.RecurringTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TaskFromAnnotationWithPropertyPathConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(TaskFromAnnotationWithPropertyPathConfiguration.class);

  @RecurringTask(
      name = "${my-custom-task.name}",
      cron = "${my-custom-task.cron}",
      zoneId = "${my-custom-task.zone-id}",
      cronStyle = "${my-custom-task.cron-style}")
  public void taskFromAnnotationWithPropertyPath() {
    log.info("I'm a task from annotation with properties path");
  }
}
