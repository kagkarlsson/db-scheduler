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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

public class TaskResolver {
	private static final Logger LOG = LoggerFactory.getLogger(TaskResolver.class);
	private final OnCannotResolve onCannotResolve;
	private final Map<String, Task> taskMap;

	public TaskResolver(List<Task> knownTasks, OnCannotResolve onCannotResolve) {
		this.onCannotResolve = onCannotResolve;
		this.taskMap = knownTasks.stream().collect(Collectors.toMap(Task::getName, identity()));
	}

	public Task resolve(String taskName) {
		Task task = taskMap.get(taskName);
		if (task == null) {
			if (onCannotResolve == OnCannotResolve.FAIL_ON_UNRESOLVED) {
				throw new RuntimeException("Unable to resolve task with name " + taskName + ".");
			} else {
				LOG.warn("Found execution with unknown task-name '{}'", taskName);
				return null;
			}
		} else {
			return task;
		}
	}

	public void addTask(Task task) {
		taskMap.put(task.getName(), task);
	}

	public enum OnCannotResolve {
		WARN_ON_UNRESOLVED,
		FAIL_ON_UNRESOLVED
	}
}
