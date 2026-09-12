package com.github.kagkarlsson.scheduler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TaskRepositoryTest {

  @Mock TaskRepository taskRepository;

  @Test
  public void default_remove_if_not_picked_only_removes_unpicked_executions() {
    TaskInstanceId id = TaskInstanceId.of("task", "1");
    Execution unpicked = new Execution(Instant.now(), new TaskInstance<>("task", "1"));
    Execution picked =
        new Execution(
            Instant.now(),
            new TaskInstance<>("task", "1"),
            true,
            "scheduler1",
            null,
            null,
            0,
            null,
            1);
    when(taskRepository.removeIfNotPicked(id)).thenCallRealMethod();
    when(taskRepository.getExecution(id))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(picked))
        .thenReturn(Optional.of(unpicked));

    assertFalse(taskRepository.removeIfNotPicked(id));
    assertFalse(taskRepository.removeIfNotPicked(id));
    assertTrue(taskRepository.removeIfNotPicked(id));

    verify(taskRepository).remove(unpicked);
    verify(taskRepository, never()).remove(picked);
  }
}
