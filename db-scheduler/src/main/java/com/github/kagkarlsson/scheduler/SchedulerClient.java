/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.jdbc.DefaultJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;

public interface SchedulerClient {

    /**
     * Schedule a new execution.
     *
     * @param taskInstance  Task-instance, optionally with data
     * @param executionTime Instant it should run
     * @return void
     * @see java.time.Instant
     * @see com.github.kagkarlsson.scheduler.task.TaskInstance
     */
    <T> void schedule(TaskInstance<T> taskInstance, Instant executionTime);

    /**
     * Update an existing execution to a new execution-time. If the execution does not exist or if it is currently
     * running, an exception is thrown.
     *
     * @param taskInstanceId
     * @param newExecutionTime the new execution-time
     * @return void
     * @see java.time.Instant
     * @see com.github.kagkarlsson.scheduler.task.TaskInstanceId
     */
    void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime);

    /**
     * Update an existing execution with a new execution-time and new task-data. If the execution does not exist or if
     * it is currently running, an exception is thrown.
     *
     * @param taskInstanceId
     * @param newExecutionTime the new execution-time
     * @param newData          the new task-data
     * @return void
     * @see java.time.Instant
     * @see com.github.kagkarlsson.scheduler.task.TaskInstanceId
     */
    <T> void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime, T newData);

    /**
     * Removes/Cancels an execution.
     *
     * @param taskInstanceId
     * @return void
     * @see com.github.kagkarlsson.scheduler.task.TaskInstanceId
     */
    void cancel(TaskInstanceId taskInstanceId);

    /**
     * Gets all scheduled executions and supplies them to the provided Consumer. A Consumer is used to
     * avoid forcing the SchedulerClient to load all executions in memory. Currently running executions are not returned.
     *
     * @param consumer Consumer for the executions
     * @return void
     */
    void getScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer);

    /**
     * Gets all scheduled executions for a task and supplies them to the provided Consumer. A Consumer is used to
     * avoid forcing the SchedulerClient to load all executions in memory. Currently running executions are not returned.
     *
     * @param taskName  the name of the task to get scheduled-executions for
     * @param dataClass the task data-class the data will be serialized and cast to
     * @param consumer  Consumer for the executions
     * @return void
     */
    <T> void getScheduledExecutionsForTask(String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer);

    /**
     * Gets the details for a specific scheduled execution. Currently running executions are also returned.
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
        private final Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
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

            TaskRepository taskRepository = new JdbcTaskRepository(
                dataSource,
                false,
                ofNullable(jdbcCustomization).orElse(new DefaultJdbcCustomization()),
                tableName,
                taskResolver,
                new SchedulerClientName(),
                serializer);

            return new StandardSchedulerClient(taskRepository);
        }
    }

    class StandardSchedulerClient implements SchedulerClient {

        private static final Logger LOG = LoggerFactory.getLogger(StandardSchedulerClient.class);
        protected final TaskRepository taskRepository;
        private SchedulerClientEventListener schedulerClientEventListener;

        StandardSchedulerClient(TaskRepository taskRepository) {
            this(taskRepository, SchedulerClientEventListener.NOOP);
        }

        StandardSchedulerClient(TaskRepository taskRepository, SchedulerClientEventListener schedulerClientEventListener) {
            this.taskRepository = taskRepository;
            this.schedulerClientEventListener = schedulerClientEventListener;
        }

        @Override
        public <T> void schedule(TaskInstance<T> taskInstance, Instant executionTime) {
            boolean success = taskRepository.createIfNotExists(new Execution(executionTime, taskInstance));
            if (success) {
                notifyListeners(ClientEvent.EventType.SCHEDULE, taskInstance, executionTime);
            }
        }

        @Override
        public void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime) {
            reschedule(taskInstanceId, newExecutionTime, null);
        }

        @Override
        public <T> void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime, T newData) {
            String taskName = taskInstanceId.getTaskName();
            String instanceId = taskInstanceId.getId();
            Optional<Execution> execution = taskRepository.getExecution(taskName, instanceId);
            if (execution.isPresent()) {
                if (execution.get().isPicked()) {
                    throw new RuntimeException(String.format("Could not reschedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
                }

                boolean success;
                if (newData == null) {
                    success = taskRepository.reschedule(execution.get(), newExecutionTime, null, null, 0);
                } else {
                    success = taskRepository.reschedule(execution.get(), newExecutionTime, newData, null, null, 0);
                }

                if (success) {
                    notifyListeners(ClientEvent.EventType.RESCHEDULE, taskInstanceId, newExecutionTime);
                }
            } else {
                throw new RuntimeException(String.format("Could not reschedule - no task with name '%s' and id '%s' was found.", taskName, instanceId));
            }
        }

        @Override
        public void cancel(TaskInstanceId taskInstanceId) {
            String taskName = taskInstanceId.getTaskName();
            String instanceId = taskInstanceId.getId();
            Optional<Execution> execution = taskRepository.getExecution(taskName, instanceId);
            if (execution.isPresent()) {
                if (execution.get().isPicked()) {
                    throw new RuntimeException(String.format("Could not cancel schedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
                }

                taskRepository.remove(execution.get());
                notifyListeners(ClientEvent.EventType.CANCEL, taskInstanceId, execution.get().executionTime);
            } else {
                throw new RuntimeException(String.format("Could not cancel schedule - no task with name '%s' and id '%s' was found.", taskName, instanceId));
            }
        }

        @Override
        public void getScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer) {
            taskRepository.getScheduledExecutions(execution -> consumer.accept(new ScheduledExecution<>(Object.class, execution)));
        }

        @Override
        public <T> void getScheduledExecutionsForTask(String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer) {
            taskRepository.getScheduledExecutions(taskName, execution -> consumer.accept(new ScheduledExecution<>(dataClass, execution)));
        }

        @Override
        public Optional<ScheduledExecution<Object>> getScheduledExecution(TaskInstanceId taskInstanceId) {
            Optional<Execution> e = taskRepository.getExecution(taskInstanceId.getTaskName(), taskInstanceId.getId());
            return e.map(oe -> new ScheduledExecution<>(Object.class, oe));
        }

        private void notifyListeners(ClientEvent.EventType eventType, TaskInstanceId taskInstanceId, Instant executionTime) {
            try {
                schedulerClientEventListener.newEvent(new ClientEvent(new ClientEvent.ClientEventContext(eventType, taskInstanceId, executionTime)));
            } catch (Exception e) {
                LOG.error("Error when notifying SchedulerClientEventListener.", e);
            }
        }
    }


    class SchedulerClientName implements SchedulerName {
        @Override
        public String getName() {
            return "SchedulerClient";
        }

    }

}
