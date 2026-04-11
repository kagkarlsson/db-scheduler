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

import com.github.kagkarlsson.scheduler.TaskResolver.UnresolvedTask;
import com.github.kagkarlsson.scheduler.event.AbstractSchedulerListener;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler listener that handles unresolved tasks during rolling updates in LOCK_AND_FETCH .
 *
 * <p>When a task type becomes unresolved (e.g., not registered in the current scheduler instance
 * due to a rolling update where new versions don't have the task yet), this listener automatically
 * releases (unpicks) those tasks so they can be processed by other scheduler instances that have
 * the task type available (this behavior is already built-in for FETCH mode out of the box).
 *
 * @see com.github.kagkarlsson.scheduler.TaskResolver
 * @see
 *     com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository#unpickUnresolved(java.util.Collection)
 */
class UnresolvedTasksLockAndFetchListener extends AbstractSchedulerListener {

  private static final Logger LOG =
      LoggerFactory.getLogger(UnresolvedTasksLockAndFetchListener.class);
  private final JdbcTaskRepository schedulerTaskRepository;
  private final TaskResolver taskResolver;

  private final List<String> refusedTasks = new CopyOnWriteArrayList<>();

  public UnresolvedTasksLockAndFetchListener(
      JdbcTaskRepository schedulerTaskRepository, TaskResolver taskResolver) {
    this.schedulerTaskRepository = schedulerTaskRepository;
    this.taskResolver = taskResolver;
  }

  @Override
  public void onSchedulerEvent(SchedulerEventType type) {
    if (type == SchedulerEventType.UNRESOLVED_TASK) {
      final List<String> unresolvedTaskNames =
          taskResolver.getUnresolved().stream()
              .map(UnresolvedTask::getTaskName)
              .filter(taskName -> !refusedTasks.contains(taskName))
              .toList();
      unpickUnresolvedTasks(unresolvedTaskNames);
    }
  }

  private void unpickUnresolvedTasks(List<String> unresolvedTaskNames) {
    if (unresolvedTaskNames.isEmpty()) {
      LOG.debug("No new unresolved tasks to unpick");
      return;
    }

    try {
      final List<String> unpickedTaskNames =
          schedulerTaskRepository.unpickUnresolved(unresolvedTaskNames);
      refusedTasks.addAll(unpickedTaskNames);
      LOG.debug("Unresolved tasks unpicked: {}", unpickedTaskNames);
    } catch (Exception ex) {
      LOG.error("Error occurred during task unpicking", ex);
    }
  }
}
