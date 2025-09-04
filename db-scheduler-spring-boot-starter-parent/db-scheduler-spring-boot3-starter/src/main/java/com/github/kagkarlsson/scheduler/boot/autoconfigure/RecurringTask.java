package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Scheduled tasks are created from the methods that are marked with this annotation. The method
 * must follow these rules: - it returns void - it has 0, 1 or 2 inputs with the following types:
 * {@link com.github.kagkarlsson.scheduler.task.TaskInstance}, generic is ignored and considered
 * Void {@link com.github.kagkarlsson.scheduler.task.ExecutionContext}
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RecurringTask {

  String name();

  String cron();
}
