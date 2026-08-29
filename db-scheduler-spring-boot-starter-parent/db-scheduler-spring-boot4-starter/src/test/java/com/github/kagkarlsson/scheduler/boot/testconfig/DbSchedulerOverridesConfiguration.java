package com.github.kagkarlsson.scheduler.boot.testconfig;

import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerOverrides;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class DbSchedulerOverridesConfiguration {

  public static final AtomicBoolean SCHEDULER_NAME_USED = new AtomicBoolean();

  @Bean
  DbSchedulerOverrides dbSchedulerOverrides() {
    return DbSchedulerOverrides.builder()
        .schedulerName(
            () -> {
              SCHEDULER_NAME_USED.set(true);
              return "recorded-scheduler-name";
            })
        .build();
  }
}
