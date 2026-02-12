package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.boot.config.RecurringTask;
import com.github.kagkarlsson.scheduler.task.schedule.CronStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TaskFromAnnotationWithCronStyleConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(TaskFromAnnotationWithCronStyleConfiguration.class);

  @RecurringTask(
      name = "taskFromAnnotationWithCronStyle",
      cron = "0 0 7 19 * ?",
      cronStyle = CronStyle.QUARTZ)
  public void taskFromAnnotationWithCronStyle() {
    log.info("I'm a task from annotation with cron style QUARTZ");
  }
}
