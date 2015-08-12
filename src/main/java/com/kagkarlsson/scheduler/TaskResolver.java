package com.kagkarlsson.scheduler;

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
		this.taskMap = knownTasks.stream().collect(Collectors.toMap((Task t) -> t.name, identity()));
	}

	public Task resolve(String taskName) {
		Task task = taskMap.get(taskName);
		if (task == null) {
			if (onCannotResolve == OnCannotResolve.FAIL_ON_UNRESOLVED) {
				throw new RuntimeException("Unable to resolve task with name " + taskName + ". Will not fetch due executions.");
			} else {
				LOG.warn("Found execution with unknown task-name '{}'", taskName);
				return null;
			}
		} else {
			return task;
		}
	}

	enum OnCannotResolve {
		WARN_ON_UNRESOLVED,
		FAIL_ON_UNRESOLVED;
	}
}
