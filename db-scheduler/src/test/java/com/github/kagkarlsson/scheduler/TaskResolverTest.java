package com.github.kagkarlsson.scheduler;

import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.github.kagkarlsson.scheduler.TaskResolver.UnresolvedTask;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class TaskResolverTest {

  private final SchedulerListener mockScheduleListener = mock(SchedulerListener.class);
  private final TaskResolver taskResolver =
      new TaskResolver(new SchedulerListeners(mockScheduleListener), new SystemClock(), List.of());

  @Test
  void shouldProduceUnresolvedTaskEventWhenTheTaskResolverIsUnableToResolveTheTask() {
    taskResolver.resolve(Resolvable.of("unresolved", Instant.now()));
    verify(mockScheduleListener).onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);
  }

  @Test
  void shouldReturnUnresolvedTasksWhenRequested() {
    final Instant now = Instant.now();
    taskResolver.resolve(Resolvable.of("unresolved1", now));
    taskResolver.resolve(Resolvable.of("unresolved2", now.plus(ofSeconds(10))));

    final List<UnresolvedTask> unresolved = taskResolver.getUnresolved();
    assertThat(unresolved, hasSize(2));
    assertThat(
        unresolved.stream().map(TaskResolver.UnresolvedTask::getTaskName).toList(),
        contains("unresolved1", "unresolved2"));
  }

  @Test
  void shouldReturnUnresolvedTasksOlderThanSpecifiedDuration() {
    final Instant now = Instant.now();
    final Instant oldTime = now.minus(ofSeconds(20));
    final Instant newTime = now.minus(ofSeconds(5));

    taskResolver.resolve(Resolvable.of("old-unresolved", oldTime));
    taskResolver.resolve(Resolvable.of("new-unresolved", newTime));

    final List<String> oldUnresolvedNames = taskResolver.getUnresolvedTaskNames(ofSeconds(10));
    assertThat(oldUnresolvedNames, contains("old-unresolved"));
  }

  @Test
  void shouldClearUnresolvedTaskWhenRequested() {
    final Instant now = Instant.now();
    taskResolver.resolve(Resolvable.of("unresolved", now));
    assertThat(taskResolver.getUnresolved(), hasSize(1));

    taskResolver.clearUnresolved("unresolved");
    assertThat(taskResolver.getUnresolved(), empty());
  }

  @Test
  void shouldNotClearOtherUnresolvedTasksWhenClearingOne() {
    final Instant now = Instant.now();
    taskResolver.resolve(Resolvable.of("unresolved1", now));
    taskResolver.resolve(Resolvable.of("unresolved2", now));

    taskResolver.clearUnresolved("unresolved1");

    final List<UnresolvedTask> remaining = taskResolver.getUnresolved();
    assertThat(remaining, hasSize(1));
    assertThat(remaining.get(0).getTaskName(), is("unresolved2"));
  }
}
