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

import com.github.kagkarlsson.scheduler.jdbc.DeactivationUpdate;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.ScheduledTaskInstance;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public interface TaskRepository {

  /** Prefer ScheduledTaskInstance which has a fixed execution-time */
  @Deprecated
  boolean createIfNotExists(SchedulableInstance<?> execution);

  boolean createIfNotExists(ScheduledTaskInstance execution);

  List<Execution> getDue(Instant now, int limit);

  void createBatch(List<ScheduledTaskInstance> executions);

  Instant replace(Execution toBeReplaced, ScheduledTaskInstance newInstance);

  /** Prefer ScheduledTaskInstance which has a fixed execution-time */
  @Deprecated
  Instant replace(Execution toBeReplaced, SchedulableInstance<?> newInstance);

  void getScheduledExecutions(ScheduledExecutionsFilter filter, Consumer<Execution> consumer);

  default List<Execution> getScheduledExecutions(ScheduledExecutionsFilter filter) {
    var results = new ArrayList<Execution>();
    getScheduledExecutions(filter, results::add);
    return results;
  }

  void getScheduledExecutions(
      ScheduledExecutionsFilter filter, String taskName, Consumer<Execution> consumer);

  default List<Execution> getScheduledExecutions(
      ScheduledExecutionsFilter filter, String taskName) {
    var results = new ArrayList<Execution>();
    getScheduledExecutions(filter, taskName, results::add);
    return results;
  }

  void getDeactivatedExecutions(Consumer<Execution> consumer);

  default List<Execution> getDeactivatedExecutions() {
    var results = new ArrayList<Execution>();
    getDeactivatedExecutions(results::add);
    return results;
  }

  List<Execution> lockAndFetchGeneric(Instant now, int limit);

  List<Execution> lockAndGetDue(Instant now, int limit);

  void remove(Execution execution);

  void deactivate(Execution execution, DeactivationUpdate deactivationUpdate);

  boolean reschedule(
      Execution execution,
      Instant nextExecutionTime,
      Instant lastSuccess,
      Instant lastFailure,
      int consecutiveFailures);

  boolean reschedule(
      Execution execution,
      Instant nextExecutionTime,
      Object newData,
      Instant lastSuccess,
      Instant lastFailure,
      int consecutiveFailures);

  Optional<Execution> pick(Execution e, Instant timePicked);

  List<Execution> getDeadExecutions(Instant olderThan);

  boolean updateHeartbeatWithRetry(Execution execution, Instant newHeartbeat, int tries);

  boolean updateHeartbeat(Execution execution, Instant heartbeatTime);

  List<Execution> getExecutionsFailingLongerThan(Duration interval);

  Optional<Execution> getExecution(String taskName, String taskInstanceId);

  default Optional<Execution> getExecution(TaskInstanceId taskInstance) {
    return getExecution(taskInstance.getTaskName(), taskInstance.getId());
  }

  int removeExecutions(String taskName);

  int removeOldDeactivatedExecutions(Set<State> states, Instant olderThan, int limit);

  void verifySupportsLockAndFetch();
}
