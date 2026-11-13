package com.github.kagkarlsson.scheduler.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.github.kagkarlsson.scheduler.TaskRepository;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ClockHandlersTest {
  private static final Instant NOW = Instant.parse("2000-01-01T00:00:00Z");

  @Test
  void replacement_uses_scheduler_clock() {
    var handlers =
        List.of(
            new CompletionHandler.OnCompleteReplace<Void, String>("next"),
            new CompletionHandler.OnCompleteReplace<Void, String>("next", "data"),
            new CompletionHandler.OnCompleteReplace<Void, String>(
                TaskDescriptor.of("next", String.class), "data"));
    for (var handler : handlers) {
      var repository = mock(TaskRepository.class);
      var completed = ExecutionComplete.simulatedSuccess(NOW.minusSeconds(1));
      var operations =
          new ExecutionOperations<Void>(
              repository, SchedulerListeners.NOOP, completed.getExecution(), () -> NOW);
      handler.complete(completed, operations);
      var replacement = ArgumentCaptor.forClass(ScheduledTaskInstance.class);
      verify(repository).replace(eq(completed.getExecution()), replacement.capture());
      assertThat(replacement.getValue().getExecutionTime()).isEqualTo(NOW);
      assertThat(replacement.getValue().getTaskName()).isEqualTo("next");
      assertThat(replacement.getValue().getId())
          .isEqualTo(completed.getExecution().taskInstance.getId());
    }
  }

  @Test
  void custom_replacement_keeps_explicit_time() {
    var repository = mock(TaskRepository.class);
    var completed = ExecutionComplete.simulatedSuccess(NOW);
    var scheduled = NOW.plusSeconds(60);
    var handler =
        new CompletionHandler.OnCompleteReplace<Void, String>(
            current ->
                SchedulableInstance.of(
                    new TaskInstance<>("next", current.getId(), "data"), scheduled));
    handler.complete(
        completed,
        new ExecutionOperations<>(
            repository, SchedulerListeners.NOOP, completed.getExecution(), () -> NOW));
    var replacement = ArgumentCaptor.forClass(ScheduledTaskInstance.class);
    verify(repository).replace(eq(completed.getExecution()), replacement.capture());
    assertThat(replacement.getValue().getExecutionTime()).isEqualTo(scheduled);
    assertThat(replacement.getValue().getTaskInstance().getData()).isEqualTo("data");
  }

  @Test
  void revival_uses_dead_execution_detection_time() {
    var repository = mock(TaskRepository.class);
    var execution = ExecutionComplete.simulatedSuccess(NOW).getExecution();
    var completed = ExecutionComplete.failure(execution, NOW, NOW, null);
    new DeadExecutionHandler.ReviveDeadExecution<Void>()
        .deadExecution(
            completed,
            new ExecutionOperations<>(repository, SchedulerListeners.NOOP, execution, () -> NOW));
    verify(repository)
        .reschedule(execution, NOW, execution.lastSuccess, NOW, execution.consecutiveFailures + 1);
  }
}
