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

import static java.time.Instant.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.FailureHandler.MaxRetriesFailureHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FailureHandlerTest {
  @Nested
  class MaxRetriesFailureHandlerTest {
    private final OneTimeTask<Integer> task =
        TestTasks.oneTime("some-task", Integer.class, (instance, executionContext) -> {});

    private final AtomicBoolean failureHandlerCalled = new AtomicBoolean(false);
    private final FailureHandler<String> failureHandler =
        (executionComplete, executionOperations) -> failureHandlerCalled.set(true);
    private final AtomicBoolean maxRetriesExceededHandlerCalled = new AtomicBoolean(false);
    private final BiConsumer<ExecutionComplete, ExecutionOperations<String>>
        maxRetriesExceededHandler =
            (executionComplete, stringExecutionOperations) ->
                maxRetriesExceededHandlerCalled.set(true);

    private final ExecutionComplete executionComplete = mock(ExecutionComplete.class);
    private final ExecutionOperations<String> executionOperations = mock(ExecutionOperations.class);

    @Test
    void should_run_handler_when_max_retries_exceeded() {
      MaxRetriesFailureHandler<String> maxRetriesFailureHandler =
          new MaxRetriesFailureHandler<>(3, failureHandler, maxRetriesExceededHandler);

      Execution execution = getExecutionWithFails(3);
      when(executionComplete.getExecution()).thenReturn(execution);

      maxRetriesFailureHandler.onFailure(executionComplete, executionOperations);

      assertThat(failureHandlerCalled.get(), is(false));
      assertThat(maxRetriesExceededHandlerCalled.get(), is(true));
      verify(executionOperations).stop();
    }

    @Test
    void should_do_not_run_max_retries_handler_when_retries_are_not_exceeded() {
      MaxRetriesFailureHandler<String> maxRetriesFailureHandler =
          new MaxRetriesFailureHandler<>(3, failureHandler, maxRetriesExceededHandler);

      Execution execution = getExecutionWithFails(1);
      when(executionComplete.getExecution()).thenReturn(execution);

      maxRetriesFailureHandler.onFailure(executionComplete, executionOperations);

      assertThat(failureHandlerCalled.get(), is(true));
      assertThat(maxRetriesExceededHandlerCalled.get(), is(false));
      verify(executionOperations, never()).stop();
    }

    private Execution getExecutionWithFails(int consecutiveFailures) {
      return new Execution(
          now(), task.instance("1"), false, null, null, null, consecutiveFailures, null, 1L);
    }
  }
}
