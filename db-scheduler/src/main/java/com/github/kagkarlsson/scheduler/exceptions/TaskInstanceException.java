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
package com.github.kagkarlsson.scheduler.exceptions;

import java.io.Serial;

public class TaskInstanceException extends DbSchedulerException {
  @Serial private static final long serialVersionUID = -2132850112553296791L;
  private static final String TASK_NAME_INSTANCE_MESSAGE_PART = " (task name: %s, instance id: %s)";

  private final String taskName;
  private final String instanceId;

  public TaskInstanceException(String message, String taskName, String instanceId, Throwable ex) {
    super(message + TASK_NAME_INSTANCE_MESSAGE_PART.formatted(taskName, instanceId), ex);
    this.taskName = taskName;
    this.instanceId = instanceId;
  }

  public TaskInstanceException(String message, String taskName, String instanceId) {
    super(message + TASK_NAME_INSTANCE_MESSAGE_PART.formatted(taskName, instanceId));
    this.taskName = taskName;
    this.instanceId = instanceId;
  }

  public String getTaskName() {
    return taskName;
  }

  public String getInstanceId() {
    return instanceId;
  }
}
