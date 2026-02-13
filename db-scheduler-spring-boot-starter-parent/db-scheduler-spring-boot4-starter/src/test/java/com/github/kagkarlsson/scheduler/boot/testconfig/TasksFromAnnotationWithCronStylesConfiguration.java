package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.boot.config.RecurringTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TasksFromAnnotationWithCronStylesConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(TasksFromAnnotationWithCronStylesConfiguration.class);

  @RecurringTask(
      name = "taskFromAnnotationWithCronStyleQuartz",
      cron = "0 0 7 19 * ?",
      cronStyle = "QUARTZ")
  public void taskFromAnnotationWithCronStyle() {}

  @RecurringTask(
      name = "taskFromAnnotationWithCronStyleCron4j",
      cron = "0 7 19 * *",
      cronStyle = "CRON4J")
  public void taskFromAnnotationWithCronStyleCron4j() {}

  @RecurringTask(
      name = "taskFromAnnotationWithCronStyleUnix",
      cron = "0 7 19 * *",
      cronStyle = "UNIX")
  public void taskFromAnnotationWithCronStyleUnix() {}

  @RecurringTask(
      name = "taskFromAnnotationWithCronStyleSpring",
      cron = "0 0 7 19 * *",
      cronStyle = "SPRING")
  public void taskFromAnnotationWithCronStyleSpring() {}

  @RecurringTask(
      name = "taskFromAnnotationWithCronStyleSpring53",
      cron = "0 0 7 19 * ?",
      cronStyle = "SPRING53")
  public void taskFromAnnotationWithCronStyleSpring53() {}
}
