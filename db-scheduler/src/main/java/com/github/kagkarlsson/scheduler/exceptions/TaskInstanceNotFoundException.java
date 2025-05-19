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

public class TaskInstanceNotFoundException extends TaskInstanceException {
  @Serial private static final long serialVersionUID = -3604591431614052358L;

  public TaskInstanceNotFoundException(String taskName, String instanceId) {
    super("Failed to perform action on task because it was not found.", taskName, instanceId);
  }
}
