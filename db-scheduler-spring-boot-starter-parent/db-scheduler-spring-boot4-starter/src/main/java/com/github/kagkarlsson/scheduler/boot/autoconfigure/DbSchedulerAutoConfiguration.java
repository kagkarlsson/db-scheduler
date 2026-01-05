/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import com.github.kagkarlsson.scheduler.Clock;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SystemClock;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerConfigurationSupport;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerCustomizer;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerProperties;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerStarter;
import com.github.kagkarlsson.scheduler.boot.config.startup.ContextReadyStart;
import com.github.kagkarlsson.scheduler.boot.config.startup.ImmediateStart;
import com.github.kagkarlsson.scheduler.event.ExecutionInterceptor;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.Task;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.LazyInitializationExcludeFilter;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(DbSchedulerProperties.class)
@AutoConfigurationPackage
@AutoConfigureAfter({
  DataSourceAutoConfiguration.class,
})
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(value = "db-scheduler.enabled", matchIfMissing = true)
public class DbSchedulerAutoConfiguration {
  private static final Logger log = LoggerFactory.getLogger(DbSchedulerAutoConfiguration.class);
  private final DbSchedulerProperties config;
  private final DataSource existingDataSource;
  private final List<Task<?>> configuredTasks;
  private final List<SchedulerListener> schedulerListeners;
  private final List<ExecutionInterceptor> executionInterceptors;

  public DbSchedulerAutoConfiguration(
      DbSchedulerProperties dbSchedulerProperties,
      DataSource dataSource,
      List<Task<?>> configuredTasks,
      List<SchedulerListener> schedulerListeners,
      List<ExecutionInterceptor> executionInterceptors) {
    this.config =
        Objects.requireNonNull(
            dbSchedulerProperties, "Can't configure db-scheduler without required configuration");
    this.existingDataSource =
        Objects.requireNonNull(dataSource, "An existing javax.sql.DataSource is required");
    this.configuredTasks =
        Objects.requireNonNull(configuredTasks, "At least one Task must be configured");
    this.schedulerListeners = schedulerListeners;
    this.executionInterceptors = executionInterceptors;
  }

  /** Provide an empty customizer if not present in the context. */
  @ConditionalOnMissingBean
  @Bean
  public DbSchedulerCustomizer noopCustomizer() {
    return new DbSchedulerCustomizer() {};
  }

  /** Will typically be created if Spring Boot Actuator is not on the classpath. */
  @ConditionalOnMissingBean(StatsRegistry.class)
  @Bean
  StatsRegistry noopStatsRegistry() {
    log.debug("Missing StatsRegistry bean in context, creating a no-op StatsRegistry");
    return StatsRegistry.NOOP;
  }

  @ConditionalOnMissingBean
  @Bean("dbSchedulerClock")
  public Clock clock() {
    return new SystemClock();
  }

  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean
  @DependsOnDatabaseInitialization
  @Bean(destroyMethod = "stop")
  public Scheduler scheduler(
      DbSchedulerCustomizer customizer, StatsRegistry registry, Clock clock) {
    log.info("Creating db-scheduler using tasks from Spring context: {}", configuredTasks);
    return DbSchedulerConfigurationSupport.buildScheduler(
        config,
        customizer,
        registry,
        clock,
        existingDataSource,
        configuredTasks,
        schedulerListeners,
        executionInterceptors);
  }

  @ConditionalOnBean(Scheduler.class)
  @ConditionalOnMissingBean
  @Bean
  public DbSchedulerStarter dbSchedulerStarter(Scheduler scheduler) {
    if (config.isDelayStartupUntilContextReady()) {
      return new ContextReadyStart(scheduler);
    }

    return new ImmediateStart(scheduler);
  }

  @Bean
  static LazyInitializationExcludeFilter eagerDbSchedulerStarter() {
    return LazyInitializationExcludeFilter.forBeanTypes(DbSchedulerStarter.class);
  }
}
