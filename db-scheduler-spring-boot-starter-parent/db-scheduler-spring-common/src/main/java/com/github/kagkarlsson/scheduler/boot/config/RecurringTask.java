package com.github.kagkarlsson.scheduler.boot.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Scheduled tasks are created from the methods that are marked with this annotation. The method
 * must follow these rules: - it must be public - it returns void - it has 0, 1 or 2 inputs with the
 * following types: - {@link com.github.kagkarlsson.scheduler.task.TaskInstance}, generic is ignored
 * and considered Void - {@link com.github.kagkarlsson.scheduler.task.ExecutionContext}
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RecurringTask {

  String name();

  /*
  The value can be either basic cron or a path to a property
  Examples:
    - ${recurring-sample-task-annotation-no-inputs.cron}
    - 0 * * * * *
   */
  String cron();

  /*
  Should be java.time.ZoneId in string representation.
  The default empty string value means default system timezone.

  It also can be a path to a property.
   */
  String zoneId() default "";

  /*
  {@see com.github.kagkarlsson.scheduler.task.schedule.CronStyle}

  It can be either a value from enum or a path to a property.
   */
  String cronStyle() default "SPRING53";
}
