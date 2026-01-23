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

/**
 * Represents the state of a task execution. A null state combined with a non-null execution_time
 * indicates an active task.
 */
public enum State {
  /** Task completed successfully and is kept for history (OneTimeTask only). */
  COMPLETE,

  /** Task is paused by user. Will not execute until resumed. Requires monitoring. */
  PAUSED,
}
