/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.exceptions;

public class TaskException extends DbSchedulerException {
    static final long serialVersionUID = -2132850112553296790L;

    private String taskName;
    private String instanceId;

    public TaskException(String message, String taskName, String instanceId, Exception ex){
        super(message, ex);
        this.taskName = taskName;
        this.instanceId = instanceId;
    }

    public TaskException(String message, String taskName, String instanceId){
        super(message + String.format(" (task name: %s, instance id: %s)", taskName, instanceId));
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
