package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class CustomStatsRegistryConfiguration {

  @Bean
  StatsRegistry customStatsRegistry() {
    return new StatsRegistry() {
      @Override
      public void register(SchedulerStatsEvent e) {}

      @Override
      public void register(CandidateStatsEvent e) {}

      @Override
      public void register(ExecutionStatsEvent e) {}

      @Override
      public void registerSingleCompletedExecution(ExecutionComplete executionComplete) {}
    };
  }
}
