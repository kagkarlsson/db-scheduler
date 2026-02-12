package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.boot.config.RecurringTask;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TasksFromAnnotationWithDifferentInputsConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(TasksFromAnnotationWithDifferentInputsConfiguration.class);

  @RecurringTask(name = "taskNoInputs", cron = "0 0 7 19 * *")
  public void taskNoInputs() {
    log.info("I'm a task without inputs");
  }

  @RecurringTask(name = "taskWithExecutionContextInput", cron = "0 0 7 19 * *")
  public void taskWithExecutionContextInput(ExecutionContext ctx) {
    log.info("I'm a task with the execution context input");
  }

  @RecurringTask(name = "taskWithTaskInstanceInput", cron = "0 0 7 19 * *")
  public void taskWithTaskInstanceInput(TaskInstance<Void> taskInstance) {
    log.info("I'm a task with the task instance input");
  }

  @RecurringTask(name = "taskWithBothInputsExpectedOrder", cron = "0 0 7 19 * *")
  public void taskWithBothInputsExpectedOrder(
      TaskInstance<Void> taskInstance, ExecutionContext ctx) {
    log.info("I'm a task with both inputs expected order");
  }

  @RecurringTask(name = "taskWithBothInputsReverseOrder", cron = "0 0 7 19 * *")
  public void taskWithBothInputsReverseOrder(
      ExecutionContext ctx, TaskInstance<Void> taskInstance) {
    log.info("I'm a task with both inputs reverse order");
  }
}
