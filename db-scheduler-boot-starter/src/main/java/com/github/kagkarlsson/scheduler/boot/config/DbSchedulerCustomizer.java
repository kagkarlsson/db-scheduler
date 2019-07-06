package com.github.kagkarlsson.scheduler.boot.config;

import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.Serializer;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Provides functionality for customizing various aspects of the DB Scheduler configuration that
 * is not easily done with properties.
 */
public interface DbSchedulerCustomizer {
  /**
   * Provide a custom {@link SchedulerName} implementation.
   */
  default Optional<SchedulerName> schedulerName() {
    return Optional.empty();
  }

  /**
   * A custom serializer for task data.
   */
  default Optional<Serializer> serializer() {
    return Optional.empty();
  }

  /**
   * Provide an existing {@link ExecutorService} instance.
   */
  default Optional<ExecutorService> executorService() {
    return Optional.empty();
  }
}
