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
package com.github.kagkarlsson.scheduler.event;

import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.ExecutionHandler;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ExecutionChain {

  private final List<ExecutionInterceptor> interceptors;
  private final ExecutionHandler<?> executionHandler;

  public ExecutionChain(
      List<ExecutionInterceptor> interceptors, ExecutionHandler<?> executionHandler) {
    this.interceptors = interceptors;
    this.executionHandler = executionHandler;
  }

  public CompletionHandler<?> proceed(
      TaskInstance taskInstance, ExecutionContext executionContext) {
    if (interceptors.isEmpty()) {
      return executionHandler.execute(taskInstance, executionContext);
    } else {
      ExecutionInterceptor nextInterceptor = interceptors.get(0);
      List<ExecutionInterceptor> rest = interceptors.subList(1, interceptors.size());
      return nextInterceptor.execute(
          taskInstance, executionContext, new ExecutionChain(rest, executionHandler));
    }
  }
}
