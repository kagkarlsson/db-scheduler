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

import java.util.Objects;
import java.util.Optional;

public class ScheduledExecutionsFilter {

  private Boolean pickedValue;
  private boolean includeUnresolved = false;
  private ExecutionTimeAndId afterExecution;
  private ExecutionTimeAndId beforeExecution;
  private Integer limit;

  private ScheduledExecutionsFilter() {}

  public static ScheduledExecutionsFilter all() {
    return new ScheduledExecutionsFilter().withIncludeUnresolved(true);
  }

  public static ScheduledExecutionsFilter onlyResolved() {
    return new ScheduledExecutionsFilter().withIncludeUnresolved(false);
  }

  public ScheduledExecutionsFilter withPicked(boolean pickedValue) {
    this.pickedValue = pickedValue;
    return this;
  }

  public ScheduledExecutionsFilter withIncludeUnresolved(boolean includeUnresolved) {
    this.includeUnresolved = includeUnresolved;
    return this;
  }

  public Optional<Boolean> getPickedValue() {
    return Optional.ofNullable(pickedValue);
  }

  public boolean getIncludeUnresolved() {
    return includeUnresolved;
  }

  public ScheduledExecutionsFilter limit(int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("Limit must be positive, was: " + limit);
    }
    this.limit = limit;
    return this;
  }

  public ScheduledExecutionsFilter after(ExecutionTimeAndId execution) {
    this.afterExecution = Objects.requireNonNull(execution);
    return this;
  }

  public ScheduledExecutionsFilter before(ExecutionTimeAndId execution) {
    this.beforeExecution = Objects.requireNonNull(execution);
    return this;
  }

  public Optional<ExecutionTimeAndId> getAfterExecution() {
    return Optional.ofNullable(afterExecution);
  }

  public Optional<ExecutionTimeAndId> getBeforeExecution() {
    return Optional.ofNullable(beforeExecution);
  }

  public Optional<Integer> getLimit() {
    return Optional.ofNullable(limit);
  }
}
