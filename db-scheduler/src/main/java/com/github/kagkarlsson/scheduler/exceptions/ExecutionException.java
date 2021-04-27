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

import com.github.kagkarlsson.scheduler.task.Execution;

public class ExecutionException extends TaskInstanceException {
    private static final long serialVersionUID = -4732028463501966553L;
    private final long version;

    public ExecutionException(String message, Execution execution){
        super(message, execution.taskInstance.getTaskName(), execution.taskInstance.getId());
        this.version = execution.version;
    }

    public ExecutionException(String message, Execution execution, Throwable ex){
        super(message, execution.taskInstance.getTaskName(), execution.taskInstance.getId(), ex);
        this.version = execution.version;
    }

    public long getVersion() {
        return version;
    }
}
