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

import java.time.Instant;

/**
 * Summary statistics for all executions sharing a task-name, intended for rendering task-centric
 * dashboards without pulling every execution into memory.
 *
 * <p>{@code runningCount}, {@code failingCount} and {@code scheduledCount} are a non-overlapping
 * partition of {@code instanceCount} (they sum to it): currently-picked executions are {@code
 * running}, the rest are {@code failing} when they have consecutive failures and {@code scheduled}
 * otherwise.
 *
 * @param taskName the task-name the executions are grouped by
 * @param instanceCount total number of executions for the task-name
 * @param runningCount executions currently picked (being executed)
 * @param failingCount executions not picked with at least one consecutive failure
 * @param scheduledCount executions not picked with no consecutive failures
 * @param earliestExecutionTime soonest next execution-time across the executions, null if none
 * @param latestLastSuccess most-recent successful execution across the executions, null if none
 * @param latestLastFailure most-recent failed execution across the executions, null if none
 * @param maxConsecutiveFailures highest consecutive-failure count across the executions
 */
public record TaskSummary(
    String taskName,
    int instanceCount,
    int runningCount,
    int failingCount,
    int scheduledCount,
    Instant earliestExecutionTime,
    Instant latestLastSuccess,
    Instant latestLastFailure,
    int maxConsecutiveFailures) {}
