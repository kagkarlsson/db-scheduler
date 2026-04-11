package com.github.kagkarlsson.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.kagkarlsson.scheduler.TaskResolver.UnresolvedTask;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskResolverTest {

  @Mock SchedulerListener mockScheduleListener;

  @Test
  void shouldProduceUnresolvedTaskEventWhenTheTaskResolverIsUnableToResolveTheTask() {
    TaskResolver taskResolver =
        new TaskResolver(
            new SchedulerListeners(mockScheduleListener), new SystemClock(), List.of());

    taskResolver.resolve(new Execution(Instant.now(), new TaskInstance<Void>("unresolved", "id1")));

    verify(mockScheduleListener, times(1)).onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);
  }

  @Test
  void shouldReturnUnresolvedTasksWhenRequested() {
    TaskResolver taskResolver =
        new TaskResolver(
            new SchedulerListeners(mockScheduleListener), new SystemClock(), List.of());

    final Instant now = Instant.now();
    taskResolver.resolve(new Execution(now, new TaskInstance<Void>("unresolved1", "id1")));
    taskResolver.resolve(
        new Execution(
            now.plus(Duration.ofSeconds(10)), new TaskInstance<Void>("unresolved2", "id2")));

    final List<UnresolvedTask> unresolved = taskResolver.getUnresolved();
    assertThat(unresolved, hasSize(2));
    assertThat(
        unresolved.stream().map(TaskResolver.UnresolvedTask::getTaskName).toList(),
        contains("unresolved1", "unresolved2"));
  }

  @Test
  void shouldTrackFirstUnresolvedExecutionTime() {
    TaskResolver taskResolver =
        new TaskResolver(
            new SchedulerListeners(mockScheduleListener), new SystemClock(), List.of());

    final Instant firstTime = Instant.now();
    final Instant secondTime = firstTime.plus(Duration.ofMinutes(5));

    taskResolver.resolve(new Execution(firstTime, new TaskInstance<Void>("unresolved", "id1")));
    taskResolver.resolve(new Execution(secondTime, new TaskInstance<Void>("unresolved", "id1")));

    final List<UnresolvedTask> unresolved = taskResolver.getUnresolved();
    assertThat(unresolved, hasSize(1));
    assertThat(unresolved.get(0).getFirstUnresolved(), is(firstTime));
  }

  @Test
  void shouldReturnUnresolvedTasksOlderThanSpecifiedDuration() {
    TaskResolver taskResolver =
        new TaskResolver(
            new SchedulerListeners(mockScheduleListener), new SystemClock(), List.of());

    final Instant now = Instant.now();
    final Instant oldTime = now.minus(Duration.ofSeconds(20));
    final Instant newTime = now.minus(Duration.ofSeconds(5));

    taskResolver.resolve(new Execution(oldTime, new TaskInstance<Void>("old-unresolved", "id1")));
    taskResolver.resolve(new Execution(newTime, new TaskInstance<Void>("new-unresolved", "id2")));

    final List<String> oldUnresolvedNames =
        taskResolver.getUnresolvedTaskNames(Duration.ofSeconds(10));
    assertThat(oldUnresolvedNames, contains("old-unresolved"));
  }

  @Test
  void shouldClearUnresolvedTaskWhenRequested() {
    final TaskResolver taskResolver =
        new TaskResolver(
            new SchedulerListeners(mockScheduleListener), new SystemClock(), List.of());

    final Instant now = Instant.now();
    taskResolver.resolve(new Execution(now, new TaskInstance<Void>("unresolved", "id1")));
    assertThat(taskResolver.getUnresolved(), hasSize(1));

    taskResolver.clearUnresolved("unresolved");
    assertThat(taskResolver.getUnresolved(), empty());
  }

  @Test
  void shouldNotClearOtherUnresolvedTasksWhenClearingOne() {
    final TaskResolver taskResolver =
        new TaskResolver(
            new SchedulerListeners(mockScheduleListener), new SystemClock(), List.of());

    final Instant now = Instant.now();
    taskResolver.resolve(new Execution(now, new TaskInstance<Void>("unresolved1", "id1")));
    taskResolver.resolve(new Execution(now, new TaskInstance<Void>("unresolved2", "id2")));

    taskResolver.clearUnresolved("unresolved1");

    final List<UnresolvedTask> remaining = taskResolver.getUnresolved();
    assertThat(remaining, hasSize(1));
    assertThat(remaining.get(0).getTaskName(), is("unresolved2"));
  }
}
