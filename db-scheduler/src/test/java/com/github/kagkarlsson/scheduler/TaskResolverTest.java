package com.github.kagkarlsson.scheduler;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskResolverTest {

  @Mock SchedulerListeners mockScheduleListeners;

  @Test
  void shouldProduceUnresolvedTaskEventWhenTheTaskResolverIsUnableToResolveTheTask() {
    TaskResolver taskResolver = new TaskResolver(mockScheduleListeners, List.of());

    taskResolver.resolve("unknown-task");

    verify(mockScheduleListeners, times(1)).onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);
  }
}
