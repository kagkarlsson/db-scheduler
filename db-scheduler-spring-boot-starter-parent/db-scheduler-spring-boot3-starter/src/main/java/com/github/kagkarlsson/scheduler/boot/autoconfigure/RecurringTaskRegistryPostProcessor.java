package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import static com.github.kagkarlsson.scheduler.boot.config.RecurringTaskRegistrySupport.createTaskFromMethod;
import static com.github.kagkarlsson.scheduler.boot.config.RecurringTaskRegistrySupport.validateMethod;

import com.github.kagkarlsson.scheduler.boot.config.RecurringTask;
import com.github.kagkarlsson.scheduler.boot.config.RecurringTaskRegistrySupport.RecurringTaskResolved;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.lang.reflect.Method;
import java.time.ZoneId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link BeanDefinitionRegistryPostProcessor} that scans for methods annotated with {@link
 * RecurringTask} and registers them as {@link Task}s in the Spring context.
 *
 * <p>The methods must have a return type of {@code void} and can accept parameters of type {@link
 * TaskInstance}, {@link ExecutionContext}.
 */
public class RecurringTaskRegistryPostProcessor implements BeanDefinitionRegistryPostProcessor {

  private static final Logger log =
      LoggerFactory.getLogger(RecurringTaskRegistryPostProcessor.class);

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
                validateMethod(method);
                RootBeanDefinition taskDef =
                    buildTaskBeanDefinition(recurringTask, method, beanName);
                registry.registerBeanDefinition(recurringTask.name(), taskDef);
              }
            },
            method -> method.isAnnotationPresent(RecurringTask.class));
      } catch (ClassNotFoundException ignored) {
      }
    }
  }

  private RootBeanDefinition buildTaskBeanDefinition(
      RecurringTask recurringTask, Method method, String beanName) {
    RootBeanDefinition taskDef = new RootBeanDefinition();
    taskDef.setBeanClass(Task.class);
    taskDef.setInstanceSupplier(
        () -> {
          RecurringTaskResolved recurringTaskResolved = resolveAnnotation(recurringTask);
          log.info(
              "Creating a task from @RecurringTask with name={}, cron={}, timezone={} ",
              recurringTask.name(),
              recurringTaskResolved.cron(),
              recurringTaskResolved.zoneId());
          return createTaskFromMethod(recurringTaskResolved, method, context.getBean(beanName));
        });
    return taskDef;
  }

  private RecurringTaskResolved resolveAnnotation(RecurringTask task) {
    String resolveCron = resolveCron(task.cron());
    ZoneId resolvedZoneId = resolveZoneId(task.zoneId());
    return new RecurringTaskResolved(task.name(), resolveCron, resolvedZoneId);
  }

  private String resolveCron(String rawCron) {
    if (rawCron.startsWith("$")) {
      return context.getEnvironment().resolveRequiredPlaceholders(rawCron);
    } else {
      return rawCron;
    }
  }

  private ZoneId resolveZoneId(String rawZoneId) {
    return StringUtils.hasLength(rawZoneId) ? ZoneId.of(rawZoneId) : ZoneId.systemDefault();
  }

  @Override
  public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory)
      throws BeansException {
    // No-op
  }
}
