package com.github.kagkarlsson.scheduler.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.helper.TestableListener;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class DeadExecutionTest {

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  @Test
  public void test_dead_execution() {
    Assertions.assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          CustomTask<Void> customTask =
              Tasks.custom("custom-a", Void.class)
                  .execute(
                      (taskInstance, executionContext) ->
                          new CompletionHandler<Void>() {
                            @Override
                            public void complete(
                                ExecutionComplete executionComplete,
                                ExecutionOperations<Void> executionOperations) {
                              // do nothing on complete, row will be left as-is in database
                            }
                          });

          TestableListener.Condition completedCondition = TestableListener.Conditions.completed(2);

          TestableListener listener =
              TestableListener.create().waitConditions(completedCondition).build();

          Scheduler scheduler =
              Scheduler.create(postgres.getDataSource(), customTask)
                  .pollingInterval(Duration.ofMillis(100))
                  .heartbeatInterval(Duration.ofMillis(100))
                  .schedulerName(new SchedulerName.Fixed("test"))
                  .addSchedulerListener(listener)
                  .build();
          stopScheduler.register(scheduler);

          scheduler.schedule(customTask.instance("1"), Instant.now());
          scheduler.start();
          completedCondition.waitFor();

          assertEquals(listener.getCount(SchedulerEventType.DEAD_EXECUTION), 1);
        });
  }
}
