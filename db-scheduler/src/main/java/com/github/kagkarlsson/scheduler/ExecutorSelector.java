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

import com.github.kagkarlsson.scheduler.task.Execution;
import java.util.List;

/** Utility class for selecting the most appropriate executor for a given execution. */
public class ExecutorSelector {

  /**
   * Find the most appropriate executor for the given execution based on its priority. Uses
   * highest-threshold routing with fallback to lower-priority pools when higher pools are at
   * capacity.
   *
   * <p>Algorithm: 1. Find all eligible executors (threshold <= task priority) 2. Sort by threshold
   * descending (highest first) 3. Return the first executor that has capacity 4. If no executor has
   * capacity, return the highest-threshold eligible executor
   *
   * @param execution the execution to find an executor for
   * @param executors the list of available executors
   * @param defaultExecutor the default executor to use if no eligible executors are found
   * @return the most appropriate executor for the execution
   */
  public static Executor findExecutorForExecution(
      Execution execution, List<Executor> executors, Executor defaultExecutor) {
    int priority = execution.taskInstance.getPriority();

    // Find all eligible executors and sort by priority threshold (highest first)
    List<Executor> eligibleExecutors =
        executors.stream()
            .filter(executor -> executor.canExecute(execution))
            .sorted(
                (e1, e2) -> Integer.compare(e2.getPriorityThreshold(), e1.getPriorityThreshold()))
            .toList();

    if (eligibleExecutors.isEmpty()) {
      return defaultExecutor;
    }

    // Try to find an executor with capacity, starting with highest priority
    for (Executor executor : eligibleExecutors) {
      if (executor.hasCapacity()) {
        return executor;
      }
    }

    // If no executor has capacity, use the highest-priority eligible executor
    // (tasks will queue but at least they'll be in the right priority pool)
    return eligibleExecutors.get(0);
  }
}
