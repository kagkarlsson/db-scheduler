package com.github.kagkarlsson.scheduler.boot.testsupport;

import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerStarter;
import com.github.kagkarlsson.scheduler.boot.config.startup.AbstractSchedulerStarter;
import com.github.kagkarlsson.scheduler.boot.config.startup.ContextReadyStart;
import com.github.kagkarlsson.scheduler.boot.config.startup.ImmediateStart;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Common test configurations for Spring Boot starter integration tests.
 */
public final class TestTasksConfiguration {
  private static final Logger log = LoggerFactory.getLogger(TestTasksConfiguration.class);

  private TestTasksConfiguration() {}

  @Configuration(proxyBeanMethods = false)
  public static class SingleTaskConfiguration {
    @Bean("singleStringTask")
    public Task<String> singleStringTask() {
      return namedTask("single-string-task");
    }
  }

  @Configuration(proxyBeanMethods = false)
  public static class MultipleTasksConfiguration {
    @Bean
    public Task<String> firstTask() {
      return namedTask("first-task");
    }

    @Bean
    public Task<String> secondTask() {
      return namedTask("second-task");
    }

    @Bean
    public Task<String> thirdTask() {
      return namedTask("third-task");
    }
  }

  @Configuration(proxyBeanMethods = false)
  public static class CustomStarterConfiguration {
    @Bean
    public DbSchedulerStarter customStarter(com.github.kagkarlsson.scheduler.Scheduler scheduler) {
      return new DelayedStart(scheduler);
    }
  }

  @Configuration(proxyBeanMethods = false)
  public static class CustomStatsRegistryConfiguration {
    @Bean
    public com.github.kagkarlsson.scheduler.stats.StatsRegistry customStatsRegistry() {
      return new com.github.kagkarlsson.scheduler.stats.StatsRegistry() {
        @Override
        public void register(com.github.kagkarlsson.scheduler.stats.StatsRegistry.SchedulerStatsEvent e) {}

        @Override
        public void register(com.github.kagkarlsson.scheduler.stats.StatsRegistry.CandidateStatsEvent e) {}

        @Override
        public void register(com.github.kagkarlsson.scheduler.stats.StatsRegistry.ExecutionStatsEvent e) {}

        @Override
        public void registerSingleCompletedExecution(
            com.github.kagkarlsson.scheduler.task.ExecutionComplete e) {}
      };
    }
  }

  private static Task<String> namedTask(String name) {
    return Tasks.recurring(name, java.time.Duration.ofMinutes(5))
        .execute((instance, ctx) -> log.info("Executing test task {}" + (name)));
  }

  private static final class DelayedStart extends AbstractSchedulerStarter {
    private DelayedStart(com.github.kagkarlsson.scheduler.Scheduler scheduler) {
      super(scheduler);
      try {
        Thread.sleep(100);
        doStart();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
