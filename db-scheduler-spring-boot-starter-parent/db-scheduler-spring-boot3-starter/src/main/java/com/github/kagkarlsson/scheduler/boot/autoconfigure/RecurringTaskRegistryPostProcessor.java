package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.util.ReflectionUtils;

/**
 * A {@link BeanDefinitionRegistryPostProcessor} that scans for methods annotated with {@link
 * RecurringTask} and registers them as {@link Task}s in the Spring context.
 *
 * <p>The methods must have a return type of {@code void} and can accept parameters of type {@link
 * TaskInstance}, {@link ExecutionContext}, or any other Spring bean.
 */
public class RecurringTaskRegistryPostProcessor implements BeanDefinitionRegistryPostProcessor {

  private static final Logger log =
      LoggerFactory.getLogger(RecurringTaskRegistryPostProcessor.class);

  private static final Set<Class<?>> INPUT_ARGUMENTS_AVAILABLE_CLASSES =
      Set.of(TaskInstance.class, ExecutionContext.class);

  private final GenericApplicationContext context;

  public RecurringTaskRegistryPostProcessor(GenericApplicationContext context) {
    this.context = context;
  }

  @Override
  public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry)
      throws BeansException {
    String[] beanNames = registry.getBeanDefinitionNames();
    for (String beanName : beanNames) {
      var beanDef = registry.getBeanDefinition(beanName);
      String beanClassName = beanDef.getBeanClassName();
      if (beanClassName == null) {
        continue;
      }
      try {
        Class<?> beanClass = Class.forName(beanClassName);
        ReflectionUtils.doWithMethods(
            beanClass,
            method -> {
              RecurringTask recurringTask = method.getAnnotation(RecurringTask.class);
              if (recurringTask != null) {
                validateInputs(method);
                GenericBeanDefinition taskDef =
                    buildTaskBeanDefinition(recurringTask, method, beanName);
                registry.registerBeanDefinition(recurringTask.name(), taskDef);
              }
            },
            method -> method.isAnnotationPresent(RecurringTask.class));
      } catch (ClassNotFoundException ignored) {
      }
    }
  }

  private GenericBeanDefinition buildTaskBeanDefinition(
      RecurringTask recurringTask, Method method, String beanName) {
    GenericBeanDefinition taskDef = new GenericBeanDefinition();
    taskDef.setBeanClass(Task.class);
    taskDef.setInstanceSupplier(
        () -> {
          log.info(
              "Creating a task from @RecurringTask with name={} and cron={}",
              recurringTask.name(),
              recurringTask.cron());
          return createTaskFromMethod(recurringTask, method, context.getBean(beanName));
        });
    return taskDef;
  }

  private void validateInputs(Method method) {
    if (!method.getReturnType().equals(Void.TYPE)) {
      throw new IllegalArgumentException(
          "RecurringTask annotated method must have return type Void, see the annotation javadoc");
    }

    if (method.getParameterCount() != 0) {
      for (int i = 0; i < method.getParameterTypes().length; i++) {
        Class<?> parameterType = method.getParameterTypes()[i];
        if (!INPUT_ARGUMENTS_AVAILABLE_CLASSES.contains(parameterType)
            && context.getBeanNamesForType(parameterType).length == 0) {
          throw new IllegalArgumentException(
              "RecurringTask annotated method is required to have specific inputs: TaskInstance, ExecutionContext or a Spring bean");
        }
      }
    }
  }

  private Task<?> createTaskFromMethod(
      RecurringTask recurringTask, Method method, Object existingObject) {

    return Tasks.recurring(recurringTask.name(), Schedules.cron(recurringTask.cron()))
        .execute(
            (instance, ctx) -> {
              Object[] inputs =
                  Arrays.stream(method.getParameterTypes())
                      .map(
                          paramType -> {
                            if (paramType.equals(TaskInstance.class)) {
                              return instance;
                            }
                            if (paramType.equals(ExecutionContext.class)) {
                              return ctx;
                            }
                            return context.getBean(paramType);
                          })
                      .toArray();
              try {
                ReflectionUtils.makeAccessible(method);
                method.invoke(existingObject, inputs);
              } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(
                    "An error happened while calling a task method through reflection, method name: "
                        + method.getName(),
                    e);
              }
            });
  }

  @Override
  public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory)
      throws BeansException {
    // No-op
  }
}
