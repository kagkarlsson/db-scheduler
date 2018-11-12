/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

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

public interface SchedulerClient {

	<T> void schedule(TaskInstance<T> taskInstance, Instant executionTime);

	void reschedule(TaskInstanceId taskInstanceId, Instant newExecutionTime);

	void cancel(TaskInstanceId taskInstanceId);

	void getScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer);

	<T> void getScheduledExecutionsForTask(String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer);

	<T> Optional<ScheduledExecution<T>> getScheduledExecution(TaskInstanceId taskInstance, Class<T> dataClass);

	class Builder {

		private final DataSource dataSource;
		private List<Task<?>> knownTasks;
		private final Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
		private String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;

		private Builder(DataSource dataSource, List<Task<?>> knownTasks) {
			this.dataSource = dataSource;
			this.knownTasks = knownTasks;
		}

		public static Builder create(DataSource dataSource, Task<?> ... knownTasks) {
			return new Builder(dataSource, Arrays.asList(knownTasks));
		}

		public static Builder create(DataSource dataSource, List<Task<?>> knownTasks) {
			return new Builder(dataSource, knownTasks);
		}

		public Builder tableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public SchedulerClient build() {
			TaskResolver taskResolver = new TaskResolver(knownTasks);
			TaskRepository taskRepository = new JdbcTaskRepository(dataSource, tableName, taskResolver, new SchedulerClientName(), serializer);
			
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
			String taskName = taskInstanceId.getTaskName();
			String instanceId = taskInstanceId.getId();
			Optional<Execution> execution = taskRepository.getExecution(taskInstanceId);
			if(execution.isPresent()) {
			    if(execution.get().isPicked()) {
                    throw new RuntimeException(String.format("Could not reschedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
                }

				boolean success = taskRepository.reschedule(execution.get(), newExecutionTime, null, null, 0);
				if (success) {
					notifyListeners(ClientEvent.EventType.RESCHEDULE, taskInstanceId, newExecutionTime);
				}
			} else {
				throw new RuntimeException(String.format("Could not reschedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
			}
		}

		@Override
		public void cancel(TaskInstanceId taskInstanceId) {
			String taskName = taskInstanceId.getTaskName();
			String instanceId = taskInstanceId.getId();
			Optional<Execution> execution = taskRepository.getExecution(taskInstanceId);
			if(execution.isPresent()) {
                if(execution.get().isPicked()) {
                    throw new RuntimeException(String.format("Could not cancel schedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
                }

                taskRepository.remove(execution.get());
				notifyListeners(ClientEvent.EventType.CANCEL, taskInstanceId, execution.get().executionTime);
			} else {
				throw new RuntimeException(String.format("Could not cancel schedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
			}
		}

		@Override
		public <T> Optional<ScheduledExecution<T>> getScheduledExecution(TaskInstanceId taskInstance, Class<T> dataClass) {
			Optional<Execution> execution = taskRepository.getExecution(taskInstance);
			return execution.map(e -> new ScheduledExecution<T>(dataClass, e));
		}

		@Override
		public void getScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer) {
			taskRepository.getScheduledExecutions(execution -> consumer.accept(new ScheduledExecution<>(Object.class, execution)));
		}

		@Override
		public <T> void getScheduledExecutionsForTask(String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer) {
			taskRepository.getScheduledExecutions(taskName, execution -> consumer.accept(new ScheduledExecution<>(dataClass, execution)));
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
