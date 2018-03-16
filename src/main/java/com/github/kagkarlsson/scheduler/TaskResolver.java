/**
 * Copyright (C) Gustav Karlsson
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

public class TaskResolver {
    private static final Logger LOG = LoggerFactory.getLogger(TaskResolver.class);
    private final Map<String, Task> taskMap;

    public TaskResolver(Task... knownTasks) {
        this(Arrays.asList(knownTasks));
    }

    public TaskResolver(List<Task> knownTasks) {
        this.taskMap = knownTasks.stream().collect(Collectors.toMap(Task::getName, identity()));
    }

    public Optional<Task> resolve(String taskName) {
        Task task = taskMap.get(taskName);
        if (task == null) {
            LOG.warn("Found execution with unknown task-name '{}'", taskName);
        }
        return Optional.ofNullable(task);
    }

    public void addTask(Task task) {
        taskMap.put(task.getName(), task);
    }

}
