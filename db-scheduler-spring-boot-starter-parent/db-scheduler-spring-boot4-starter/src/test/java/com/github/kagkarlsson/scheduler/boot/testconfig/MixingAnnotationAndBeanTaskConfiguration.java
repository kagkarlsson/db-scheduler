package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.boot.autoconfigure.RecurringTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MixingAnnotationAndBeanTaskConfiguration extends MultipleTasksConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(MixingAnnotationAndBeanTaskConfiguration.class);

  @RecurringTask(name = "taskFromAnnotation", cron = "0 0 7 19 * *")
  public void taskFromAnnotation() {
    log.info("I'm a task from annotation");
  }
}
