package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerStarter;
import com.github.kagkarlsson.scheduler.boot.config.startup.AbstractSchedulerStarter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class CustomStarterConfiguration {

  @Bean
  DbSchedulerStarter delayedStarter(Scheduler scheduler) {
    return new AbstractSchedulerStarter(scheduler) {
      {
        try {
          Thread.sleep(100);
          doStart();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
  }
}
