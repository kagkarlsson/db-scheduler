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
package com.github.kagkarlsson.scheduler.task;

import com.github.kagkarlsson.scheduler.task.TaskInstance.Builder;

public abstract class AbstractTask<T> implements Task<T> {

  protected final String name;
  private final FailureHandler<T> failureHandler;
  private final DeadExecutionHandler<T> deadExecutionHandler;
  private final Class<T> dataClass;

  private final int defaultPriority;

  public AbstractTask(
      String name,
      Class<T> dataClass,
      FailureHandler<T> failureHandler,
      DeadExecutionHandler<T> deadExecutionHandler) {
    this(name, dataClass, failureHandler, deadExecutionHandler, DEFAULT_PRIORITY);
  }

  public AbstractTask(
      String name,
      Class<T> dataClass,
      FailureHandler<T> failureHandler,
      DeadExecutionHandler<T> deadExecutionHandler,
      int defaultPriority) {
    this.name = name;
    this.dataClass = dataClass;
    this.failureHandler = failureHandler;
    this.deadExecutionHandler = deadExecutionHandler;
    this.defaultPriority = defaultPriority;
  }

  @Override
  public int getDefaultPriority() {
    return defaultPriority;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Class<T> getDataClass() {
    return dataClass;
  }

  @Override
  public TaskInstance<T> instance(String id) {
    return instanceBuilder(id).build();
  }

  @Override
  public TaskInstance<T> instance(String id, T data) {
    return instanceBuilder(id).data(data).build();
  }

  @Override
  public TaskInstance.Builder<T> instanceBuilder(String id) {
    return new Builder<T>(this.name, id).priority(getDefaultPriority());
  }

  @Override
  public FailureHandler<T> getFailureHandler() {
    return failureHandler;
  }

  @Override
  public DeadExecutionHandler<T> getDeadExecutionHandler() {
    return deadExecutionHandler;
  }

  @Override
  public String toString() {
    return "Task " + "name=" + getName();
  }
}
