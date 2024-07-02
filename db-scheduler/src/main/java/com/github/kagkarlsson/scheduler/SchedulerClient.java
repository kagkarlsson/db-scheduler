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
package com.github.kagkarlsson.scheduler;

import static java.util.Optional.ofNullable;

import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceCurrentlyExecutingException;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceNotFoundException;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SchedulerClient {

  /**
   * Schedule a new execution.
   *
   * @param taskInstance Task-instance, optionally with data
   * @param executionTime Instant it should run
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstance
   */
  <T> void schedule(TaskInstance<T> taskInstance, Instant executionTime);

  <T> void schedule(SchedulableInstance<T> schedulableInstance);

  /**
   * Update an existing execution to a new execution-time. If the execution does not exist or if it
   * is currently running, an exception is thrown.
   *
   * @param taskInstanceId
   * @param newExecutionTime the new execution-time
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstanceId
   */
  void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime);

  /**
   * Update an existing execution with a new execution-time and new task-data. If the execution does
   * not exist or if it is currently running, an exception is thrown.
   *
   * @param taskInstanceId
   * @param newExecutionTime the new execution-time
   * @param newData the new task-data
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstanceId
   */
  <T> void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime, T newData);

  /**
   * Update an existing execution with a new execution-time and new task-data. If the execution does
   * not exist or if it is currently running, an exception is thrown.
   *
   * @param schedulableInstance the updated instance
   */
  <T> void reschedule(SchedulableInstance<T> schedulableInstance);

  /**
   * Removes/Cancels an execution.
   *
   * @param taskInstanceId
   * @see com.github.kagkarlsson.scheduler.task.TaskInstanceId
   */
  void cancel(TaskInstanceId taskInstanceId);

  /**
   * Gets all scheduled executions and supplies them to the provided Consumer. A Consumer is used to
   * avoid forcing the SchedulerClient to load all executions in memory. Currently running
   * executions are not returned.
   *
   * @param consumer Consumer for the executions
   */
  void fetchScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer);

  void fetchScheduledExecutions(
      ScheduledExecutionsFilter filter, Consumer<ScheduledExecution<Object>> consumer);

  /**
   * @see #fetchScheduledExecutions(Consumer)
   */
  default List<ScheduledExecution<Object>> getScheduledExecutions() {
    List<ScheduledExecution<Object>> executions = new ArrayList<>();
    fetchScheduledExecutions(executions::add);
    return executions;
  }

  /**
   * @see #fetchScheduledExecutions(Consumer)
   */
  default List<ScheduledExecution<Object>> getScheduledExecutions(
      ScheduledExecutionsFilter filter) {
    List<ScheduledExecution<Object>> executions = new ArrayList<>();
    fetchScheduledExecutions(filter, executions::add);
    return executions;
  }

  /**
   * Gets all scheduled executions for a task and supplies them to the provided Consumer. A Consumer
   * is used to avoid forcing the SchedulerClient to load all executions in memory. Currently
   * running executions are not returned.
   *
   * @param taskName the name of the task to get scheduled-executions for
   * @param dataClass the task data-class the data will be serialized and cast to
   * @param consumer Consumer for the executions
   */
  <T> void fetchScheduledExecutionsForTask(
      String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer);

  <T> void fetchScheduledExecutionsForTask(
      String taskName,
      Class<T> dataClass,
      ScheduledExecutionsFilter filter,
      Consumer<ScheduledExecution<T>> consumer);

  /**
   * @see #fetchScheduledExecutionsForTask(String, Class, Consumer)
   */
  default <T> List<ScheduledExecution<Object>> getScheduledExecutionsForTask(String taskName) {
    List<ScheduledExecution<Object>> executions = new ArrayList<>();
    fetchScheduledExecutionsForTask(taskName, Object.class, executions::add);
    return executions;
  }

  /**
   * @see #fetchScheduledExecutionsForTask(String, Class, Consumer)
   */
  default <T> List<ScheduledExecution<T>> getScheduledExecutionsForTask(
      String taskName, Class<T> dataClass) {
    List<ScheduledExecution<T>> executions = new ArrayList<>();
    fetchScheduledExecutionsForTask(taskName, dataClass, executions::add);
    return executions;
  }

  /**
   * @see #fetchScheduledExecutionsForTask(String, Class, Consumer)
   */
  default <T> List<ScheduledExecution<T>> getScheduledExecutionsForTask(
      String taskName, Class<T> dataClass, ScheduledExecutionsFilter filter) {
    List<ScheduledExecution<T>> executions = new ArrayList<>();
    fetchScheduledExecutionsForTask(taskName, dataClass, filter, executions::add);
    return executions;
  }

  /**
   * Gets the details for a specific scheduled execution. Currently running executions are also
   * returned.
   *
   * @param taskInstanceId
   * @return Optional.empty() if no matching execution found
   * @see com.github.kagkarlsson.scheduler.task.TaskInstanceId
   * @see com.github.kagkarlsson.scheduler.ScheduledExecution
   */
  Optional<ScheduledExecution<Object>> getScheduledExecution(TaskInstanceId taskInstanceId);

  class Builder {

    private final DataSource dataSource;
    private List<Task<?>> knownTasks;
    private Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
    private String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;
    private JdbcCustomization jdbcCustomization;

    private Builder(DataSource dataSource, List<Task<?>> knownTasks) {
      this.dataSource = dataSource;
      this.knownTasks = knownTasks;
    }

    public static Builder create(DataSource dataSource, Task<?>... knownTasks) {
      return new Builder(dataSource, Arrays.asList(knownTasks));
    }

    public static Builder create(DataSource dataSource, List<Task<?>> knownTasks) {
      return new Builder(dataSource, knownTasks);
    }

    public Builder serializer(Serializer serializer) {
      this.serializer = serializer;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder jdbcCustomization(JdbcCustomization jdbcCustomization) {
      this.jdbcCustomization = jdbcCustomization;
      return this;
    }

    public SchedulerClient build() {
      TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, knownTasks);
      final SystemClock clock = new SystemClock();

      TaskRepository taskRepository =
          new JdbcTaskRepository(
              dataSource,
              false,
              ofNullable(jdbcCustomization).orElse(new AutodetectJdbcCustomization(dataSource)),
              tableName,
              taskResolver,
              new SchedulerClientName(),
              serializer,
              clock);

      return new StandardSchedulerClient(taskRepository, clock);
    }
  }

  class StandardSchedulerClient implements SchedulerClient {

    private static final Logger LOG = LoggerFactory.getLogger(StandardSchedulerClient.class);
    protected final TaskRepository taskRepository;
    private final Clock clock;
    private final SchedulerListeners schedulerListeners;

    StandardSchedulerClient(TaskRepository taskRepository, Clock clock) {
      this(taskRepository, SchedulerListeners.NOOP, clock);
    }

    StandardSchedulerClient(
        TaskRepository taskRepository,
        SchedulerListeners schedulerListeners,
        Clock clock) {
      this.taskRepository = taskRepository;
      this.schedulerListeners = schedulerListeners;
      this.clock = clock;
    }

    @Override
    public <T> void schedule(TaskInstance<T> taskInstance, Instant executionTime) {
      boolean success =
          taskRepository.createIfNotExists(SchedulableInstance.of(taskInstance, executionTime));
      if (success) {
        schedulerListeners.onExecutionScheduled(taskInstance, executionTime);
      }
    }

    @Override
    public <T> void schedule(SchedulableInstance<T> schedulableInstance) {
      schedule(
          schedulableInstance.getTaskInstance(),
          schedulableInstance.getNextExecutionTime(clock.now()));
    }

    @Override
    public void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime) {
      reschedule(taskInstanceId, newExecutionTime, null);
    }

    @Override
    public <T> void reschedule(SchedulableInstance<T> schedulableInstance) {
      reschedule(
          schedulableInstance,
          schedulableInstance.getNextExecutionTime(clock.now()),
          schedulableInstance.getTaskInstance().getData());
    }

    @Override
    public <T> void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime, T newData) {
      String taskName = taskInstanceId.getTaskName();
      String instanceId = taskInstanceId.getId();
      Optional<Execution> execution = taskRepository.getExecution(taskName, instanceId);
      if (execution.isPresent()) {
        if (execution.get().isPicked()) {
          throw new TaskInstanceCurrentlyExecutingException(taskName, instanceId);
        }

        boolean success;
        if (newData == null) {
          success = taskRepository.reschedule(execution.get(), newExecutionTime, null, null, 0);
        } else {
          success =
              taskRepository.reschedule(execution.get(), newExecutionTime, newData, null, null, 0);
        }

        if (success) {
          schedulerListeners.onExecutionScheduled(taskInstanceId, newExecutionTime);
        }
      } else {
        throw new TaskInstanceNotFoundException(taskName, instanceId);
      }
    }

    @Override
    public void cancel(TaskInstanceId taskInstanceId) {
      String taskName = taskInstanceId.getTaskName();
      String instanceId = taskInstanceId.getId();
      Optional<Execution> execution = taskRepository.getExecution(taskName, instanceId);
      if (execution.isPresent()) {
        if (execution.get().isPicked()) {
          throw new TaskInstanceCurrentlyExecutingException(taskName, instanceId);
        }

        taskRepository.remove(execution.get());
      } else {
        throw new TaskInstanceNotFoundException(taskName, instanceId);
      }
    }

    @Override
    public void fetchScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer) {
      fetchScheduledExecutions(ScheduledExecutionsFilter.all().withPicked(false), consumer);
    }

    @Override
    public void fetchScheduledExecutions(
        ScheduledExecutionsFilter filter, Consumer<ScheduledExecution<Object>> consumer) {
      taskRepository.getScheduledExecutions(
          filter, execution -> consumer.accept(new ScheduledExecution<>(Object.class, execution)));
    }

    @Override
    public <T> void fetchScheduledExecutionsForTask(
        String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer) {
      fetchScheduledExecutionsForTask(
          taskName, dataClass, ScheduledExecutionsFilter.all().withPicked(false), consumer);
    }

    @Override
    public <T> void fetchScheduledExecutionsForTask(
        String taskName,
        Class<T> dataClass,
        ScheduledExecutionsFilter filter,
        Consumer<ScheduledExecution<T>> consumer) {
      taskRepository.getScheduledExecutions(
          filter,
          taskName,
          execution -> consumer.accept(new ScheduledExecution<>(dataClass, execution)));
    }

    @Override
    public Optional<ScheduledExecution<Object>> getScheduledExecution(
        TaskInstanceId taskInstanceId) {
      Optional<Execution> e =
          taskRepository.getExecution(taskInstanceId.getTaskName(), taskInstanceId.getId());
      return e.map(oe -> new ScheduledExecution<>(Object.class, oe));
    }

  }

  class SchedulerClientName implements SchedulerName {
    @Override
    public String getName() {
      return "SchedulerClient";
    }
  }
}
