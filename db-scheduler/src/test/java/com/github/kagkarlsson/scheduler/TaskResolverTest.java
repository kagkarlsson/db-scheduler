package com.github.kagkarlsson.scheduler;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
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
}
