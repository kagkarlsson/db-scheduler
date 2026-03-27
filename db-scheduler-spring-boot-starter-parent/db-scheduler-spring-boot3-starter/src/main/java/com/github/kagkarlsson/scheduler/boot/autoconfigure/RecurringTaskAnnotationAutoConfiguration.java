package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;

@Configuration
@AutoConfigureBefore(DbSchedulerAutoConfiguration.class)
@AutoConfigureAfter({
  DataSourceAutoConfiguration.class,
})
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(value = "db-scheduler.enabled", matchIfMissing = true)
public class RecurringTaskAnnotationAutoConfiguration {

  @Bean
  public RecurringTaskRegistryPostProcessor recurringTaskRegistryPostProcessor(
      GenericApplicationContext context) {
    return new RecurringTaskRegistryPostProcessor(context);
  }
}
