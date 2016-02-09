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

import java.time.Instant;
import java.util.ArrayList;

import javax.sql.DataSource;

import com.github.kagkarlsson.scheduler.TaskResolver.OnCannotResolve;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;

public interface SchedulerClient {
	
	void scheduleForExecution(Instant exeecutionTime, TaskInstance taskInstance);
	
	class Builder {
		
		private DataSource dataSource;

		private Builder(DataSource dataSource) {
			this.dataSource = dataSource;
		}
		public static Builder create(DataSource dataSource) {
			return new Builder(dataSource);
		}
		
		public SchedulerClient build() {
			TaskResolver taskResolver = new TaskResolver(OnCannotResolve.FAIL_ON_UNRESOLVED, new ArrayList<>());
			TaskRepository taskRepository = new JdbcTaskRepository(dataSource, taskResolver, new SchedulerClientName());
			
			return new StandardSchedulerClient(taskRepository);
		}
	}
	
	class StandardSchedulerClient implements SchedulerClient {

		private TaskRepository taskRepository;
		
		StandardSchedulerClient(TaskRepository taskRepository) {
			this.taskRepository = taskRepository;
		}
		
		@Override
		public void scheduleForExecution(Instant exeecutionTime,
				TaskInstance taskInstance) {
			taskRepository.createIfNotExists(new Execution(exeecutionTime, taskInstance));
		} 
	}
	
	static class SchedulerClientName implements SchedulerName {
		@Override
		public String getName() {
			return "SchedulerClient";
		}
		
	}

}
