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

import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Overrides for the parts of the db-scheduler configuration that cannot be expressed as properties.
 *
 * <p>Expose one as a bean to have it picked up by the auto-configuration:
 *
 * <pre>{@code
 * @Bean
 * DbSchedulerOverrides dbSchedulerOverrides() {
 *   return DbSchedulerOverrides.builder()
 *       .schedulerName(new SchedulerName.Fixed("scheduler-1"))
 *       .serializer(new JacksonSerializer())
 *       .build();
 * }
 * }</pre>
 *
 * <p>Anything left unset falls back to the corresponding {@code db-scheduler.*} property, and then
 * to the library default. Settings that do have a property equivalent are deliberately absent here;
 * to set one programmatically, or to reach a {@link SchedulerBuilder} option that the starter does
 * not surface, use {@link Builder#customizeBuilder(Consumer)}.
 */
@NullMarked
public final class DbSchedulerOverrides {
  private static final DbSchedulerOverrides NONE = builder().build();

  private final @Nullable SchedulerName schedulerName;
  private final @Nullable Serializer serializer;
  private final @Nullable DataSource dataSource;
  private final @Nullable ExecutorService executorService;
  private final @Nullable ExecutorService dueExecutor;
  private final @Nullable ScheduledExecutorService housekeeperExecutor;
  private final @Nullable JdbcCustomization jdbcCustomization;
  private final @Nullable Consumer<SchedulerBuilder> builderCustomizer;

  private DbSchedulerOverrides(Builder builder) {
    this.schedulerName = builder.schedulerName;
    this.serializer = builder.serializer;
    this.dataSource = builder.dataSource;
    this.executorService = builder.executorService;
    this.dueExecutor = builder.dueExecutor;
    this.housekeeperExecutor = builder.housekeeperExecutor;
    this.jdbcCustomization = builder.jdbcCustomization;
    this.builderCustomizer = builder.builderCustomizer;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Overrides where nothing is set, leaving the configuration entirely up to the properties. */
  public static DbSchedulerOverrides none() {
    return NONE;
  }

  /** A builder pre-populated with the values of this instance, for copy-and-adjust. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** A custom {@link SchedulerName}. Takes precedence over {@code db-scheduler.scheduler-name}. */
  public Optional<SchedulerName> schedulerName() {
    return Optional.ofNullable(schedulerName);
  }

  /** A custom serializer for task data. */
  public Optional<Serializer> serializer() {
    return Optional.ofNullable(serializer);
  }

  /** A {@link DataSource} to use instead of the one found in the application context. */
  public Optional<DataSource> dataSource() {
    return Optional.ofNullable(dataSource);
  }

  /** An existing {@link ExecutorService} to use for processing tasks. */
  public Optional<ExecutorService> executorService() {
    return Optional.ofNullable(executorService);
  }

  /** An existing {@link ExecutorService} to use for handling due executions. */
  public Optional<ExecutorService> dueExecutor() {
    return Optional.ofNullable(dueExecutor);
  }

  /** An existing {@link ScheduledExecutorService} to use for housekeeping tasks. */
  public Optional<ScheduledExecutorService> housekeeperExecutor() {
    return Optional.ofNullable(housekeeperExecutor);
  }

  /** A custom {@link JdbcCustomization}. */
  public Optional<JdbcCustomization> jdbcCustomization() {
    return Optional.ofNullable(jdbcCustomization);
  }

  /** Direct access to the {@link SchedulerBuilder}, applied after everything else. */
  public Optional<Consumer<SchedulerBuilder>> builderCustomizer() {
    return Optional.ofNullable(builderCustomizer);
  }

  public static final class Builder {
    private @Nullable SchedulerName schedulerName;
    private @Nullable Serializer serializer;
    private @Nullable DataSource dataSource;
    private @Nullable ExecutorService executorService;
    private @Nullable ExecutorService dueExecutor;
    private @Nullable ScheduledExecutorService housekeeperExecutor;
    private @Nullable JdbcCustomization jdbcCustomization;
    private @Nullable Consumer<SchedulerBuilder> builderCustomizer;

    private Builder() {}

    private Builder(DbSchedulerOverrides overrides) {
      this.schedulerName = overrides.schedulerName;
      this.serializer = overrides.serializer;
      this.dataSource = overrides.dataSource;
      this.executorService = overrides.executorService;
      this.dueExecutor = overrides.dueExecutor;
      this.housekeeperExecutor = overrides.housekeeperExecutor;
      this.jdbcCustomization = overrides.jdbcCustomization;
      this.builderCustomizer = overrides.builderCustomizer;
    }

    public Builder schedulerName(SchedulerName schedulerName) {
      this.schedulerName = Objects.requireNonNull(schedulerName, "schedulerName must not be null");
      return this;
    }

    public Builder serializer(Serializer serializer) {
      this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
      return this;
    }

    public Builder dataSource(DataSource dataSource) {
      this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
      return this;
    }

    public Builder executorService(ExecutorService executorService) {
      this.executorService =
          Objects.requireNonNull(executorService, "executorService must not be null");
      return this;
    }

    public Builder dueExecutor(ExecutorService dueExecutor) {
      this.dueExecutor = Objects.requireNonNull(dueExecutor, "dueExecutor must not be null");
      return this;
    }

    public Builder housekeeperExecutor(ScheduledExecutorService housekeeperExecutor) {
      this.housekeeperExecutor =
          Objects.requireNonNull(housekeeperExecutor, "housekeeperExecutor must not be null");
      return this;
    }

    public Builder jdbcCustomization(JdbcCustomization jdbcCustomization) {
      this.jdbcCustomization =
          Objects.requireNonNull(jdbcCustomization, "jdbcCustomization must not be null");
      return this;
    }

    /**
     * Escape hatch for anything not covered above, including settings that otherwise come from
     * properties. Applied last, so it wins over both the properties and the other overrides.
     *
     * <p>Note that {@link #dataSource(DataSource)} is not reachable this way, as the DataSource is
     * an input to the builder rather than a setting on it.
     */
    public Builder customizeBuilder(Consumer<SchedulerBuilder> builderCustomizer) {
      this.builderCustomizer =
          Objects.requireNonNull(builderCustomizer, "builderCustomizer must not be null");
      return this;
    }

    public DbSchedulerOverrides build() {
      return new DbSchedulerOverrides(this);
    }
  }
}
