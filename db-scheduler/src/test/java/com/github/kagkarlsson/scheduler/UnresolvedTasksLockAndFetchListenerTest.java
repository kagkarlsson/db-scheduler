package com.github.kagkarlsson.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.kagkarlsson.scheduler.TaskResolver.UnresolvedTask;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class UnresolvedTasksLockAndFetchListenerTest {

  private final JdbcTaskRepository mockSchedulerTaskRepository = mock(JdbcTaskRepository.class);
  private final TaskResolver mockTaskResolver = mock(TaskResolver.class);

  private final UnresolvedTasksLockAndFetchListener listener =
      new UnresolvedTasksLockAndFetchListener(mockSchedulerTaskRepository, mockTaskResolver);

  @Test
  void shouldNotCallRepositoryWhenEventTypeIsNotUNRESOLVED_TASK() {
    when(mockTaskResolver.getUnresolved()).thenReturn(new ArrayList<>());

    listener.onSchedulerEvent(SchedulerEventType.RAN_EXECUTE_DUE);

    verify(mockSchedulerTaskRepository, never()).unpickUnresolved(anyCollection());
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldCallUnpickUnresolvedWhenUNRESOLVED_TASKEventFired() {
    List<UnresolvedTask> unresolvedTasks = List.of(new UnresolvedTask("Task1", Instant.now()));

    when(mockTaskResolver.getUnresolved()).thenReturn(unresolvedTasks);
    when(mockSchedulerTaskRepository.unpickUnresolved(anyCollection()))
        .thenReturn(List.of("Task1"));

    listener.onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);

    ArgumentCaptor<Collection<String>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(mockSchedulerTaskRepository).unpickUnresolved(captor.capture());

    assertThat(captor.getValue(), contains("Task1"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldFilterOutRefusedTasksFromSecondCall() {
    List<UnresolvedTask> unresolvedTasks = new ArrayList<>();
    unresolvedTasks.add(new TaskResolver.UnresolvedTask("Task1", Instant.now()));
    unresolvedTasks.add(new TaskResolver.UnresolvedTask("Task2", Instant.now()));

    when(mockTaskResolver.getUnresolved()).thenReturn(unresolvedTasks);
    when(mockSchedulerTaskRepository.unpickUnresolved(anyCollection()))
        .thenReturn(List.of("Task1"));

    // First call
    listener.onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);

    verify(mockSchedulerTaskRepository).unpickUnresolved(anyCollection());

    // Second call - Task1 should be filtered as refused
    when(mockTaskResolver.getUnresolved()).thenReturn(unresolvedTasks);
    when(mockSchedulerTaskRepository.unpickUnresolved(anyCollection())).thenReturn(List.of());

    listener.onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);

    ArgumentCaptor<Collection<String>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(mockSchedulerTaskRepository, times(2)).unpickUnresolved(captor.capture());

    // Second call should only have Task2
    assertThat(captor.getValue(), contains("Task2"));
  }

  @Test
  void shouldHandleEmptyUnresolvedListGracefully() {
    when(mockTaskResolver.getUnresolved()).thenReturn(new ArrayList<>());

    assertDoesNotThrow(() -> listener.onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK));

    verify(mockSchedulerTaskRepository, never()).unpickUnresolved(anyCollection());
  }

  @Test
  void shouldHandleRepositoryExceptionGracefully() {
    List<UnresolvedTask> unresolvedTasks = new ArrayList<>();
    unresolvedTasks.add(new TaskResolver.UnresolvedTask("Task1", Instant.now()));

    when(mockTaskResolver.getUnresolved()).thenReturn(unresolvedTasks);
    when(mockSchedulerTaskRepository.unpickUnresolved(anyCollection()))
        .thenThrow(new RuntimeException("Database error"));

    assertDoesNotThrow(() -> listener.onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK));

    verify(mockSchedulerTaskRepository).unpickUnresolved(anyCollection());
  }

  @Test
  void shouldNotRetryUnpickedTasksOnSubsequentEvents() {
    List<UnresolvedTask> unresolvedTasks = new ArrayList<>();
    unresolvedTasks.add(new TaskResolver.UnresolvedTask("Task1", Instant.now()));

    when(mockTaskResolver.getUnresolved()).thenReturn(unresolvedTasks);
    when(mockSchedulerTaskRepository.unpickUnresolved(anyCollection()))
        .thenReturn(List.of("Task1"));

    // First call - unpicks Task1
    listener.onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);

    verify(mockSchedulerTaskRepository).unpickUnresolved(anyCollection());

    // Second call with same unresolved task
    when(mockTaskResolver.getUnresolved()).thenReturn(unresolvedTasks);
    listener.onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);

    // Should not call repository again because Task1 is in refusedTasks
    verify(mockSchedulerTaskRepository).unpickUnresolved(anyCollection());
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldHandleMultipleUnresolvedTasksInSingleEvent() {
    List<UnresolvedTask> unresolvedTasks =
        List.of(
            new TaskResolver.UnresolvedTask("Task1", Instant.now()),
            new TaskResolver.UnresolvedTask("Task2", Instant.now()),
            new TaskResolver.UnresolvedTask("Task3", Instant.now()));

    when(mockTaskResolver.getUnresolved()).thenReturn(unresolvedTasks);
    when(mockSchedulerTaskRepository.unpickUnresolved(anyCollection()))
        .thenReturn(List.of("Task1", "Task2", "Task3"));

    listener.onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);

    ArgumentCaptor<Collection<String>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(mockSchedulerTaskRepository).unpickUnresolved(captor.capture());

    assertThat(captor.getValue(), hasSize(3));
    assertThat(captor.getValue(), contains("Task1", "Task2", "Task3"));
  }
}
