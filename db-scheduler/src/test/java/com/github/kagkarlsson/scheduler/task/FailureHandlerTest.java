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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.FailureHandler.MaxRetriesFailureHandler;
import com.github.kagkarlsson.scheduler.task.FailureHandler.SequenceFailureHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  @Nested
  class SequenceFailureHandlerTest {

    private final OneTimeTask<Integer> task =
        TestTasks.oneTime("some-task", Integer.class, (instance, executionContext) -> {});

    private final AtomicInteger primaryFailureHandlerCallCount = new AtomicInteger(0);
    private final FailureHandler<String> primaryFailureHandler =
        (executionComplete, executionOperations) ->
            primaryFailureHandlerCallCount.incrementAndGet();

    private final AtomicBoolean secondaryFailureHandlerCalled = new AtomicBoolean(false);
    private final FailureHandler<String> secondaryFailureHandler =
        (executionComplete, executionOperations) -> secondaryFailureHandlerCalled.set(true);

    private final ExecutionComplete executionComplete = mock(ExecutionComplete.class);
    private final ExecutionOperations<String> executionOperations = mock(ExecutionOperations.class);

    @Test
    void should_run_secondary_handler_when_primary_handler_fails() {
      SequenceFailureHandler<String> sequenceFailureHandler =
          new SequenceFailureHandler<>(primaryFailureHandler, secondaryFailureHandler);

      // First failure
      Execution execution = getExecutionWithFails(0);
      when(executionComplete.getExecution()).thenReturn(execution);

      sequenceFailureHandler.onFailure(executionComplete, executionOperations);

      assertThat(primaryFailureHandlerCallCount.get(), is(1));
      assertThat(secondaryFailureHandlerCalled.get(), is(false));

      // Second failure
      execution = getExecutionWithFails(1);
      when(executionComplete.getExecution()).thenReturn(execution);

      sequenceFailureHandler.onFailure(executionComplete, executionOperations);

      assertThat(primaryFailureHandlerCallCount.get(), is(1));
      assertThat(secondaryFailureHandlerCalled.get(), is(true));
    }

    @Test
    void should_retry_primary_handler_according_to_the_primaryHandlerRetryCount() {
      int primaryHandlerRetryCount = 3;
      SequenceFailureHandler<String> sequenceFailureHandler =
          new SequenceFailureHandler<>(
              primaryFailureHandler, secondaryFailureHandler, primaryHandlerRetryCount);

      // First failure
      Execution execution = getExecutionWithFails(0);
      when(executionComplete.getExecution()).thenReturn(execution);

      sequenceFailureHandler.onFailure(executionComplete, executionOperations);

      assertThat(primaryFailureHandlerCallCount.get(), is(1));
      assertThat(secondaryFailureHandlerCalled.get(), is(false));

      // Second failure
      execution = getExecutionWithFails(1);
      when(executionComplete.getExecution()).thenReturn(execution);

      sequenceFailureHandler.onFailure(executionComplete, executionOperations);

      assertThat(primaryFailureHandlerCallCount.get(), is(2));
      assertThat(secondaryFailureHandlerCalled.get(), is(false));

      // Third failure
      execution = getExecutionWithFails(2);
      when(executionComplete.getExecution()).thenReturn(execution);

      sequenceFailureHandler.onFailure(executionComplete, executionOperations);

      assertThat(primaryFailureHandlerCallCount.get(), is(3));
      assertThat(secondaryFailureHandlerCalled.get(), is(false));

      // Fourth failure
      execution = getExecutionWithFails(3);
      when(executionComplete.getExecution()).thenReturn(execution);

      sequenceFailureHandler.onFailure(executionComplete, executionOperations);

      assertThat(primaryFailureHandlerCallCount.get(), is(3));
      assertThat(secondaryFailureHandlerCalled.get(), is(true));
    }

    @Test
    void should_throw_exception_when_primaryFailureHandler_is_MaxRetriesFailureHandler() {
      FailureHandler<String> primaryFailureHandlerForThisTest =
          new MaxRetriesFailureHandler<>(3, null, null);
      IllegalArgumentException thrown =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  new SequenceFailureHandler<>(
                      primaryFailureHandlerForThisTest, secondaryFailureHandler));
      assertThat(
          thrown.getMessage(),
          is(
              "Primary failure handler cannot be a MaxRetriesFailureHandler as it stops the execution. Use the primaryHandlerRetryCount parameter instead if retries are needed."));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void should_throw_exception_when_primaryHandlerRetryCount_is_smaller_than_1(
        int primaryHandlerRetryCount) {
      IllegalArgumentException thrown =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  new SequenceFailureHandler<>(
                      primaryFailureHandler, secondaryFailureHandler, primaryHandlerRetryCount));

      assertThat(thrown.getMessage(), is("Primary handler retry count must be at least 1."));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void should_be_successful_when_primaryHandlerRetryCount_is_1_or_greater(
        int primaryHandlerRetryCount) {
      assertDoesNotThrow(
          () ->
              new SequenceFailureHandler<>(
                  primaryFailureHandler, secondaryFailureHandler, primaryHandlerRetryCount));
    }

    private Execution getExecutionWithFails(int consecutiveFailures) {
      return new Execution(
          now(), task.instance("1"), false, null, null, null, consecutiveFailures, null, 1L);
    }
  }
}
