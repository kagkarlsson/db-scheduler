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
package com.github.kagkarlsson.scheduler.boot.config;

import com.github.kagkarlsson.scheduler.Clock;
import com.github.kagkarlsson.scheduler.PollingStrategyConfig;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.event.ExecutionInterceptor;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.exceptions.SerializationException;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistryAdapter;
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
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ConfigurableObjectInputStream;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

/** Helper for building {@link Scheduler} instances shared between Spring Boot starters. */
public final class DbSchedulerConfigurationSupport {
  private static final Logger log = LoggerFactory.getLogger(DbSchedulerConfigurationSupport.class);
  private static final Predicate<Task<?>> SHOULD_BE_STARTED = task -> task instanceof OnStartup;

  private DbSchedulerConfigurationSupport() {}

  /**
   * @deprecated use the {@link DbSchedulerOverrides} overload. Will be removed in a future version
   *     along with {@link DbSchedulerCustomizer}.
   */
  @Deprecated(since = "17.0.0", forRemoval = true)
  public static Scheduler buildScheduler(
      DbSchedulerProperties config,
      DbSchedulerCustomizer customizer,
      StatsRegistry registry,
      Clock clock,
      DataSource existingDataSource,
      List<Task<?>> configuredTasks,
      List<SchedulerListener> schedulerListeners,
      List<ExecutionInterceptor> executionInterceptors) {

    Objects.requireNonNull(customizer, "Customizer must not be null");
    return buildScheduler(
        config,
        toOverrides(customizer),
        registry,
        clock,
        existingDataSource,
        configuredTasks,
        schedulerListeners,
        executionInterceptors);
  }

  public static Scheduler buildScheduler(
      DbSchedulerProperties config,
      DbSchedulerOverrides overrides,
      StatsRegistry registry,
      Clock clock,
      DataSource existingDataSource,
      List<Task<?>> configuredTasks,
      List<SchedulerListener> schedulerListeners,
      List<ExecutionInterceptor> executionInterceptors) {

    Objects.requireNonNull(config, "Configuration must not be null");
    Objects.requireNonNull(overrides, "Overrides must not be null");
    Objects.requireNonNull(registry, "StatsRegistry must not be null");
    Objects.requireNonNull(clock, "Clock must not be null");
    Objects.requireNonNull(existingDataSource, "DataSource must not be null");
    Objects.requireNonNull(configuredTasks, "Configured tasks must not be null");
    Objects.requireNonNull(schedulerListeners, "Scheduler listeners must not be null");
    Objects.requireNonNull(executionInterceptors, "Execution interceptors must not be null");

    DataSource transactionalDataSource =
        configureDataSource(overrides.dataSource().orElse(existingDataSource));

    SchedulerBuilder builder =
        Scheduler.create(transactionalDataSource, nonStartupTasks(configuredTasks));

    builder.clock(clock);
    builder.threads(config.getThreads());

    builder.pollingInterval(config.getPollingInterval());

    if (config.getPollingStrategy() == PollingStrategyConfig.Type.FETCH) {
      builder.pollUsingFetch(
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
    builder.missedHeartbeatsLimit(config.getMissedHeartbeatsLimit());

    if (overrides.schedulerName().isPresent()) {
      builder.schedulerName(overrides.schedulerName().get());
    } else if (config.getSchedulerName() != null) {
      builder.schedulerName(new SchedulerName.Fixed(config.getSchedulerName()));
    }

    builder.tableName(config.getTableName());
    builder.serializer(overrides.serializer().orElse(SPRING_JAVA_SERIALIZER));
    overrides.jdbcCustomization().ifPresent(builder::jdbcCustomization);

    if (config.isAlwaysPersistTimestampInUtc()) {
      builder.alwaysPersistTimestampInUTC();
    }

    if (config.isImmediateExecutionEnabled()) {
      builder.enableImmediateExecution();
    }

    if (config.isPriorityEnabled()) {
      builder.enablePriority();
    }

    overrides.executorService().ifPresent(builder::executorService);
    overrides.dueExecutor().ifPresent(builder::dueExecutor);
    overrides.housekeeperExecutor().ifPresent(builder::housekeeperExecutor);

    builder.deleteUnresolvedAfter(config.getDeleteUnresolvedAfter());
    builder.startTasks(startupTasks(configuredTasks));
    builder.addSchedulerListener(new StatsRegistryAdapter(registry));
    builder.failureLogging(config.getFailureLoggerLevel(), config.isFailureLoggerLogStackTrace());
    builder.shutdownMaxWait(config.getShutdownMaxWait());

    schedulerListeners.forEach(builder::addSchedulerListener);
    executionInterceptors.forEach(builder::addExecutionInterceptor);

    // Applied last so it can override anything derived from the properties above.
    overrides.builderCustomizer().ifPresent(customize -> customize.accept(builder));

    return builder.build();
  }

  /**
   * Resolves which overrides to build the scheduler from, bridging a legacy {@link
   * DbSchedulerCustomizer} bean if that is all the context provides.
   */
  @SuppressWarnings("removal")
  public static DbSchedulerOverrides resolveOverrides(
      @Nullable DbSchedulerOverrides overrides, @Nullable DbSchedulerCustomizer legacyCustomizer) {

    if (overrides != null) {
      if (legacyCustomizer != null) {
        log.warn(
            "Found both a DbSchedulerOverrides and a deprecated DbSchedulerCustomizer bean. "
                + "The DbSchedulerCustomizer is ignored and should be removed.");
      }
      return overrides;
    }

    if (legacyCustomizer != null) {
      log.warn(
          "DbSchedulerCustomizer is deprecated and support for it will be removed in a future "
              + "version. Expose a DbSchedulerOverrides bean instead.");
      return toOverrides(legacyCustomizer);
    }

    return DbSchedulerOverrides.none();
  }

  /**
   * @deprecated bridge for {@link DbSchedulerCustomizer}, removed in a future version along with
   *     it.
   */
  @Deprecated(since = "17.0.0", forRemoval = true)
  private static DbSchedulerOverrides toOverrides(DbSchedulerCustomizer customizer) {
    DbSchedulerOverrides.Builder builder = DbSchedulerOverrides.builder();

    customizer.schedulerName().ifPresent(builder::schedulerName);
    customizer.serializer().ifPresent(builder::serializer);
    customizer.dataSource().ifPresent(builder::dataSource);
    customizer.executorService().ifPresent(builder::executorService);
    customizer.dueExecutor().ifPresent(builder::dueExecutor);
    customizer.housekeeperExecutor().ifPresent(builder::housekeeperExecutor);
    customizer.jdbcCustomization().ifPresent(builder::jdbcCustomization);

    return builder.build();
  }

  public static DataSource configureDataSource(DataSource existingDataSource) {
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
        .filter(SHOULD_BE_STARTED)
        .map(task -> (T) task)
        .collect(Collectors.toList());
  }

  private static List<Task<?>> nonStartupTasks(List<Task<?>> tasks) {
    return tasks.stream().filter(SHOULD_BE_STARTED.negate()).collect(Collectors.toList());
  }

  public static final Serializer SPRING_JAVA_SERIALIZER =
      new Serializer() {

        public byte[] serialize(Object data) {
          if (data == null) {
            return null;
          }
          try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
              ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(data);
            return bos.toByteArray();
          } catch (Exception e) {
            throw new SerializationException("Failed to serialize object", e);
          }
        }

        public <T> T deserialize(Class<T> clazz, byte[] serializedData) {
          if (serializedData == null) {
            return null;
          }
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
