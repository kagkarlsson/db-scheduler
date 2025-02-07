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

import com.github.kagkarlsson.scheduler.PollingStrategyConfig;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerCustomizer;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerProperties;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerStarter;
import com.github.kagkarlsson.scheduler.boot.config.startup.ContextReadyStart;
import com.github.kagkarlsson.scheduler.boot.config.startup.ImmediateStart;
import com.github.kagkarlsson.scheduler.event.ExecutionInterceptor;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.exceptions.SerializationException;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.LazyInitializationExcludeFilter;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ConfigurableObjectInputStream;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

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
  private static final Predicate<Task<?>> shouldBeStarted = task -> task instanceof OnStartup;

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

  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean
  @DependsOnDatabaseInitialization
  @Bean(destroyMethod = "stop")
  public Scheduler scheduler(DbSchedulerCustomizer customizer, StatsRegistry registry) {
    log.info("Creating db-scheduler using tasks from Spring context: {}", configuredTasks);

    // Ensure that we are using a transactional aware data source
    DataSource transactionalDataSource =
        configureDataSource(customizer.dataSource().orElse(existingDataSource));

    // Instantiate a new builder
    final SchedulerBuilder builder =
        Scheduler.create(transactionalDataSource, nonStartupTasks(configuredTasks));

    builder.threads(config.getThreads());

    // Polling
    builder.pollingInterval(config.getPollingInterval());

    // Polling strategy
    if (config.getPollingStrategy() == PollingStrategyConfig.Type.FETCH) {
      builder.pollUsingFetchAndLockOnExecute(
          config.getPollingStrategyLowerLimitFractionOfThreads(),
          config.getPollingStrategyUpperLimitFractionOfThreads());
    } else if (config.getPollingStrategy() == PollingStrategyConfig.Type.LOCK_AND_FETCH) {
      builder.pollUsingLockAndFetch(
          config.getPollingStrategyLowerLimitFractionOfThreads(),
          config.getPollingStrategyUpperLimitFractionOfThreads());
    } else {
      throw new IllegalArgumentException(
          "Unknown polling-strategy: " + config.getPollingStrategy());
    }

    builder.heartbeatInterval(config.getHeartbeatInterval());

    // Use scheduler name implementation from customizer if available, otherwise use
    // configured scheduler name (String). If both is absent, use the library default
    if (customizer.schedulerName().isPresent()) {
      builder.schedulerName(customizer.schedulerName().get());
    } else if (config.getSchedulerName() != null) {
      builder.schedulerName(new SchedulerName.Fixed(config.getSchedulerName()));
    }

    builder.tableName(config.getTableName());

    // Use custom serializer if provided. Otherwise use devtools friendly serializer.
    builder.serializer(customizer.serializer().orElse(SPRING_JAVA_SERIALIZER));

    // Use custom JdbcCustomizer if provided.
    customizer.jdbcCustomization().ifPresent(builder::jdbcCustomization);

    if (config.isAlwaysPersistTimestampInUtc()) {
      builder.alwaysPersistTimestampInUTC();
    }

    if (config.isImmediateExecutionEnabled()) {
      builder.enableImmediateExecution();
    }

    if (config.isPriorityEnabled()) {
      builder.enablePriority();
    }

    // Use custom executor service if provided
    customizer.executorService().ifPresent(builder::executorService);

    // Use custom due executor if provided
    customizer.dueExecutor().ifPresent(builder::dueExecutor);

    // Use housekeeper executor service if provided
    customizer.housekeeperExecutor().ifPresent(builder::housekeeperExecutor);

    builder.deleteUnresolvedAfter(config.getDeleteUnresolvedAfter());

    // Add recurring jobs and jobs that implements OnStartup
    builder.startTasks(startupTasks(configuredTasks));

    // Expose metrics
    builder.statsRegistry(registry);

    // Failure logging
    builder.failureLogging(config.getFailureLoggerLevel(), config.isFailureLoggerLogStackTrace());

    // Shutdown max wait
    builder.shutdownMaxWait(config.getShutdownMaxWait());

    // Register listeners
    schedulerListeners.forEach(builder::addSchedulerListener);

    // Register interceptors
    executionInterceptors.forEach(builder::addExecutionInterceptor);

    return builder.build();
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

  private static DataSource configureDataSource(DataSource existingDataSource) {
    if (existingDataSource instanceof TransactionAwareDataSourceProxy) {
      log.debug("Using an already transaction aware DataSource");
      return existingDataSource;
    }

    log.debug(
        "The configured DataSource is not transaction aware: '{}'. Wrapping in TransactionAwareDataSourceProxy.",
        existingDataSource);

    return new TransactionAwareDataSourceProxy(existingDataSource);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Task<?> & OnStartup> List<T> startupTasks(List<Task<?>> tasks) {
    return tasks.stream()
        .filter(shouldBeStarted)
        .map(task -> (T) task)
        .collect(Collectors.toList());
  }

  private static List<Task<?>> nonStartupTasks(List<Task<?>> tasks) {
    return tasks.stream().filter(shouldBeStarted.negate()).collect(Collectors.toList());
  }

  /**
   * {@link Serializer} compatible with Spring Boot Devtools.
   *
   * @see <a href=
   *     "https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#using-boot-devtools-known-restart-limitations">
   *     Devtools known limitations</a>
   */
  private static final Serializer SPRING_JAVA_SERIALIZER =
      new Serializer() {

        public byte[] serialize(Object data) {
          if (data == null) return null;
          try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
              ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(data);
            return bos.toByteArray();
          } catch (Exception e) {
            throw new SerializationException("Failed to serialize object", e);
          }
        }

        public <T> T deserialize(Class<T> clazz, byte[] serializedData) {
          if (serializedData == null) return null;
          try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedData);
              ObjectInput in =
                  new ConfigurableObjectInputStream(
                      bis, Thread.currentThread().getContextClassLoader())) {
            return clazz.cast(in.readObject());
          } catch (Exception e) {
            throw new SerializationException("Failed to deserialize object", e);
          }
        }
      };
}
