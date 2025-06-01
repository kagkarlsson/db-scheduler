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
import static java.util.stream.Collectors.toList;

import com.github.kagkarlsson.scheduler.SchedulerClient.ScheduleOptions.WhenExists;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceCurrentlyExecutingException;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceNotFoundException;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.ScheduledTaskInstance;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SchedulerClient {

  int DEFAULT_BATCH_SIZE = 100;

  /**
   * Schedule a new execution for the given task instance.
   *
   * <p>If the task instance already exists, the specified policy in <code>scheduleOptions</code>
   * applies.
   *
   * <p>An exception is thrown if the execution is currently running.
   *
   * @param taskInstance Task-instance, optionally with data
   * @param executionTime Instant it should run
   * @param scheduleOptions policy for when the instance exists
   * @return true if the task-instance actually was scheduled
   * @throws TaskInstanceCurrentlyExecutingException if the execution is currently running
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstance
   * @see com.github.kagkarlsson.scheduler.exceptions.TaskInstanceCurrentlyExecutingException
   */
  <T> boolean schedule(
      TaskInstance<T> taskInstance, Instant executionTime, ScheduleOptions scheduleOptions);

  /**
   * Schedule a new execution for the given task instance.
   *
   * <p>If the task instance already exists, the specified policy in <code>scheduleOptions</code>
   * applies.
   *
   * <p>An exception is thrown if the execution is currently running.
   *
   * @param schedulableInstance Task-instance and time it should run
   * @param scheduleOptions policy for when the instance exists
   * @return true if the task-instance actually was scheduled
   * @throws TaskInstanceCurrentlyExecutingException if the execution is currently running
   * @see com.github.kagkarlsson.scheduler.task.SchedulableInstance
   * @see com.github.kagkarlsson.scheduler.exceptions.TaskInstanceCurrentlyExecutingException
   */
  <T> boolean schedule(SchedulableInstance<T> schedulableInstance, ScheduleOptions scheduleOptions);

  /**
   * Schedule a new execution if task instance does not already exists.
   *
   * @param taskInstance Task-instance, optionally with data
   * @param executionTime Instant it should run
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstance
   * @deprecated use {@link #scheduleIfNotExists(TaskInstance, Instant)} instead.
   */
  @Deprecated
  <T> void schedule(TaskInstance<T> taskInstance, Instant executionTime);

  /**
   * @deprecated use {@link #scheduleIfNotExists(SchedulableInstance)} instead.
   */
  @Deprecated
  <T> void schedule(SchedulableInstance<T> schedulableInstance);

  /**
   * Schedule a new execution if task instance does not already exists.
   *
   * @param taskInstance Task-instance, optionally with data
   * @param executionTime Instant it should run
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstance
   * @return true if scheduled successfully
   */
  <T> boolean scheduleIfNotExists(TaskInstance<T> taskInstance, Instant executionTime);

  /**
   * Schedule a new execution if task instance does not already exists.
   *
   * @param schedulableInstance Task-instance and time it should run
   * @see com.github.kagkarlsson.scheduler.task.SchedulableInstance
   * @return true if scheduled successfully
   */
  <T> boolean scheduleIfNotExists(SchedulableInstance<T> schedulableInstance);

  /**
   * Schedule a batch of executions. If any of the executions already exists, the scheduling will
   * fail and an exception will be thrown.
   *
   * @param taskInstances Task-instance to schedule, optionally with data
   * @param executionTime Instant it should run
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstance
   */
  void scheduleBatch(List<TaskInstance<?>> taskInstances, Instant executionTime);

  /**
   * Schedule a batch of executions. If any of the executions already exists, the scheduling will
   * fail and an exception will be thrown.
   *
   * @param schedulableInstances Task-instances with individual execution-times
   * @see com.github.kagkarlsson.scheduler.task.SchedulableInstance
   */
  void scheduleBatch(List<SchedulableInstance<?>> schedulableInstances);

  /**
   * Schedule a batch of executions. If any of the executions already exists, the scheduling will
   * fail and an exception will be thrown.
   *
   * @param taskInstances Task-instances to schedule, optionally with data
   * @param executionTime Instant it should run
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstance
   */
  default void scheduleBatch(Stream<TaskInstance<?>> taskInstances, Instant executionTime) {
    StreamUtils.chunkStream(taskInstances, DEFAULT_BATCH_SIZE)
        .forEach(chunk -> scheduleBatch(chunk, executionTime));
  }

  /**
   * Schedule a batch of executions. If any of the executions already exists, the scheduling will
   * fail and an exception will be thrown.
   *
   * @param schedulableInstances Task-instances with individual execution-times
   * @see com.github.kagkarlsson.scheduler.task.SchedulableInstance
   */
  default void scheduleBatch(Stream<SchedulableInstance<?>> schedulableInstances) {
    StreamUtils.chunkStream(schedulableInstances, DEFAULT_BATCH_SIZE).forEach(this::scheduleBatch);
  }

  /**
   * Update an existing execution to a new execution-time. If the execution does not exist or if it
   * is currently running, an exception is thrown.
   *
   * @param taskInstanceId Task-instance to reschedule, expected to exist
   * @param newExecutionTime the new execution-time
   * @return true if rescheduled successfully
   * @throws TaskInstanceNotFoundException if the given instance does not exist
   * @throws TaskInstanceCurrentlyExecutingException if the execution is currently running
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstanceId
   */
  boolean reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime);

  /**
   * Update an existing execution with a new execution-time and new task-data. If the execution does
   * not exist or if it is currently running, an exception is thrown.
   *
   * @param taskInstanceId
   * @param newExecutionTime the new execution-time
   * @param newData the new task-data
   * @return true if rescheduled successfully
   * @throws TaskInstanceNotFoundException if the given instance does not exist
   * @throws TaskInstanceCurrentlyExecutingException if the execution is currently running
   * @see java.time.Instant
   * @see com.github.kagkarlsson.scheduler.task.TaskInstanceId
   */
  <T> boolean reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime, T newData);

  /**
   * Update an existing execution with a new execution-time and new task-data. If the execution does
   * not exist or if it is currently running, an exception is thrown.
   *
   * @param schedulableInstance the updated instance
   * @return true if rescheduled successfully
   * @throws TaskInstanceNotFoundException if the given instance does not exist
   * @throws TaskInstanceCurrentlyExecutingException if the execution is currently running
   */
  <T> boolean reschedule(SchedulableInstance<T> schedulableInstance);

  /**
   * Removes/Cancels an execution.
   *
   * @param taskInstanceId
   * @throws TaskInstanceNotFoundException if the given instance does not exist
   * @throws TaskInstanceCurrentlyExecutingException if the given instance is currently being executed
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

  class ScheduleOptions {

    public static final ScheduleOptions WHEN_EXISTS_DO_NOTHING =
        defaultOptions().whenExistsDoNothing();

    public static final ScheduleOptions WHEN_EXISTS_RESCHEDULE =
        defaultOptions().whenExistsReschedule();

    public enum WhenExists {
      RESCHEDULE,
      DO_NOTHING;
    }

    private WhenExists whenExists;

    public static ScheduleOptions defaultOptions() {
      return new ScheduleOptions();
    }

    public ScheduleOptions whenExistsReschedule() {
      this.whenExists = WhenExists.RESCHEDULE;
      return this;
    }

    public ScheduleOptions whenExistsDoNothing() {
      this.whenExists = WhenExists.DO_NOTHING;
      return this;
    }

    public WhenExists getWhenExists() {
      return whenExists;
    }
  }

  class Builder {

    private final DataSource dataSource;
    private List<Task<?>> knownTasks;
    private Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
    private String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;
    private JdbcCustomization jdbcCustomization;
    private boolean priority = false;

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

    /** Will cause getScheduledExecutions(..) to return executions in priority order. */
    public Builder enablePriority() {
      this.priority = true;
      return this;
    }

    public Builder jdbcCustomization(JdbcCustomization jdbcCustomization) {
      this.jdbcCustomization = jdbcCustomization;
      return this;
    }

    public SchedulerClient build() {
      final SystemClock clock = new SystemClock();
      TaskResolver taskResolver = new TaskResolver(SchedulerListeners.NOOP, clock, knownTasks);

      final JdbcCustomization jdbcCustomization =
          ofNullable(this.jdbcCustomization)
              .orElseGet(() -> new AutodetectJdbcCustomization(dataSource));

      TaskRepository taskRepository =
          new JdbcTaskRepository(
              dataSource,
              false,
              jdbcCustomization,
              tableName,
              taskResolver,
              new SchedulerClientName(),
              serializer,
              priority,
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
        TaskRepository taskRepository, SchedulerListeners schedulerListeners, Clock clock) {
      this.taskRepository = taskRepository;
      this.schedulerListeners = schedulerListeners;
      this.clock = clock;
    }

    @Override
    public <T> void schedule(TaskInstance<T> taskInstance, Instant executionTime) {
      // ignore result even if failed to schedule due to duplicates for backwards-compatibility
      scheduleIfNotExists(taskInstance, executionTime);
    }

    @Override
    public <T> boolean scheduleIfNotExists(TaskInstance<T> taskInstance, Instant executionTime) {
      boolean success =
          taskRepository.createIfNotExists(SchedulableInstance.of(taskInstance, executionTime));
      if (success) {
        schedulerListeners.onExecutionScheduled(taskInstance, executionTime);
      }
      return success;
    }

    @Override
    public <T> boolean scheduleIfNotExists(SchedulableInstance<T> schedulableInstance) {
      return scheduleIfNotExists(
          schedulableInstance.getTaskInstance(),
          schedulableInstance.getNextExecutionTime(clock.now()));
    }

    @Override
    public void scheduleBatch(List<TaskInstance<?>> taskInstances, Instant executionTime) {
      List<ScheduledTaskInstance> batchToSchedule =
          taskInstances.stream()
              .map(taskInstance -> new ScheduledTaskInstance(taskInstance, executionTime))
              .collect(toList());

      taskRepository.createBatch(batchToSchedule);
      notifyListenersOfScheduledBatch(batchToSchedule);
    }

    @Override
    public void scheduleBatch(List<SchedulableInstance<?>> schedulableInstances) {
      List<ScheduledTaskInstance> batchToSchedule =
          schedulableInstances.stream()
              .map(schedulable -> ScheduledTaskInstance.fixExecutionTime(schedulable, clock))
              .collect(toList());

      taskRepository.createBatch(batchToSchedule);
      notifyListenersOfScheduledBatch(batchToSchedule);
    }

    private void notifyListenersOfScheduledBatch(List<ScheduledTaskInstance> batchToSchedule) {
      batchToSchedule.forEach(
          instance ->
              schedulerListeners.onExecutionScheduled(instance, instance.getExecutionTime()));
    }

    @Override
    public <T> void schedule(SchedulableInstance<T> schedulableInstance) {
      schedule(
          schedulableInstance.getTaskInstance(),
          schedulableInstance.getNextExecutionTime(clock.now()));
    }

    @Override
    public <T> boolean schedule(
        TaskInstance<T> taskInstance, Instant executionTime, ScheduleOptions scheduleOptions) {

      boolean successfulSchedule = scheduleIfNotExists(taskInstance, executionTime);
      if (successfulSchedule) {
        return true;
      }

      WhenExists whenExists = scheduleOptions.getWhenExists();
      if (whenExists == WhenExists.DO_NOTHING) {
        // failed to schedule, but user has chosen to ignore
        LOG.debug("Task instance already exists. Keeping existing. task-instance={}", taskInstance);
        return false;

      } else if (whenExists == WhenExists.RESCHEDULE) {
        // successfulSchedule = false, whenExists = RESCHEDULE
        //   i.e. failed to schedule because it already exists -> try reschedule
        var existing = getScheduledExecution(taskInstance);
        if (existing.isEmpty()) {
          // something must have processed it and deleted it
          LOG.warn(
              "Task-instance should already exist, but failed to find it. "
                  + "It must have been processed and deleted. task-instance={}",
              taskInstance);
          return false;
        }

        LOG.debug("Task instance already exists. Rescheduling. task-instance={}", taskInstance);
        return reschedule(taskInstance, executionTime, taskInstance.getData());

      } else {
        throw new IllegalArgumentException("Unknown WhenExists value: " + whenExists);
      }
    }

    @Override
    public <T> boolean schedule(
        SchedulableInstance<T> schedulableInstance, ScheduleOptions whenExists) {
      return schedule(
          schedulableInstance.getTaskInstance(),
          schedulableInstance.getNextExecutionTime(clock.now()),
          whenExists);
    }

    @Override
    public boolean reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime) {
      return reschedule(taskInstanceId, newExecutionTime, null);
    }

    @Override
    public <T> boolean reschedule(SchedulableInstance<T> schedulableInstance) {
      return reschedule(
          schedulableInstance,
          schedulableInstance.getNextExecutionTime(clock.now()),
          schedulableInstance.getTaskInstance().getData());
    }

    @Override
    public <T> boolean reschedule(
        TaskInstanceId taskInstanceId, Instant newExecutionTime, T newData) {
      String taskName = taskInstanceId.getTaskName();
      String instanceId = taskInstanceId.getId();
      Execution execution =
          taskRepository
              .getExecution(taskName, instanceId)
              .orElseThrow(() -> new TaskInstanceNotFoundException(taskName, instanceId));

      if (execution.isPicked()) {
        throw new TaskInstanceCurrentlyExecutingException(taskName, instanceId);
      }

      boolean success;
      if (newData == null) {
        success = taskRepository.reschedule(execution, newExecutionTime, null, null, 0);
      } else {
        success = taskRepository.reschedule(execution, newExecutionTime, newData, null, null, 0);
      }

      if (success) {
        schedulerListeners.onExecutionScheduled(taskInstanceId, newExecutionTime);
      } else {
        LOG.warn("Failed to reschedule task instance: {}", taskInstanceId);
      }
      return success;
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
