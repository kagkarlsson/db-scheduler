package com.github.kagkarlsson.scheduler;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskResolverTest {

  @Mock SchedulerListener mockScheduleListener;

  @Test
  void shouldProduceUnresolvedTaskEventWhenTheTaskResolverIsUnableToResolveTheTask() {
    TaskResolver taskResolver = new TaskResolver(mockScheduleListener);

    taskResolver.resolve("unknown-task");

    verify(mockScheduleListener, times(1)).onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);
  }
}
