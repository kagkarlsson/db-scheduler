package com.github.kagkarlsson.scheduler.boot.config;

import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.CronStyle;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Set;
import org.springframework.util.StringUtils;

public final class RecurringTaskRegistrySupport {
  private static final Set<Class<?>> INPUT_ARGUMENTS_AVAILABLE_CLASSES =
      Set.of(TaskInstance.class, ExecutionContext.class);

  public static void validateMethod(Method method) {
    if (!Modifier.isPublic(method.getModifiers())) {
      throw new IllegalArgumentException(
          "RecurringTask annotated method must be public, see the annotation javadoc");
    }
    if (!method.getReturnType().equals(Void.TYPE)) {
      throw new IllegalArgumentException(
          "RecurringTask annotated method must have return type Void, see the annotation javadoc");
    }

    if (method.getParameterCount() != 0) {
      for (Class<?> parameterType : method.getParameterTypes()) {
        if (!INPUT_ARGUMENTS_AVAILABLE_CLASSES.contains(parameterType)) {
          throw new IllegalArgumentException(
              "RecurringTask annotated method is required to have specific inputs: "
                  + INPUT_ARGUMENTS_AVAILABLE_CLASSES);
        }
      }
    }
  }

  public static Task<?> createTaskFromMethod(
      RecurringTaskResolved recurringTaskResolved, Method method, Object existingObject) {

    return Tasks.recurring(
            recurringTaskResolved.name(),
            Schedules.cron(
                recurringTaskResolved.cron,
                recurringTaskResolved.zoneId,
                recurringTaskResolved.cronStyle))
        .execute(
            (instance, ctx) -> {
              Object[] inputs =
                  Arrays.stream(method.getParameterTypes())
                      .map(
                          paramType -> {
                            if (paramType.equals(TaskInstance.class)) {
                              return instance;
                            } else if (paramType.equals(ExecutionContext.class)) {
                              return ctx;
                            }
                            throw new IllegalArgumentException(
                                "RecurringTask annotated method is required to have specific inputs: "
                                    + INPUT_ARGUMENTS_AVAILABLE_CLASSES);
                          })
                      .toArray();
              try {
                method.invoke(existingObject, inputs);
              } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(
                    "An error happened while calling a task method through reflection, method name: "
                        + method.getName(),
                    e);
              }
            });
  }

  public record RecurringTaskResolved(
      String name, String cron, ZoneId zoneId, CronStyle cronStyle) {
    public static RecurringTaskResolved from(
        String name, String cron, String zoneId, String cronStyle) {
      return new RecurringTaskResolved(
          name,
          cron,
          StringUtils.hasLength(zoneId) ? ZoneId.of(zoneId) : ZoneId.systemDefault(),
          CronStyle.valueOf(cronStyle));
    }
  }
}
