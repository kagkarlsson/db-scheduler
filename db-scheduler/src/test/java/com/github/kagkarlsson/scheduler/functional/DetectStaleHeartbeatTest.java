package com.github.kagkarlsson.scheduler.functional;

import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.scheduler.CurrentlyExecuting;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.HeartbeatState;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks.PausingHandler;
import com.github.kagkarlsson.scheduler.helper.TestableListener;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler.CancelDeadExecution;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetectStaleHeartbeatTest {
  private static final Logger LOG = LoggerFactory.getLogger(DetectStaleHeartbeatTest.class);

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  @RepeatedTest(10) // FIXLATER: remove when test is stable
  public void test_dead_execution() throws InterruptedException {
    PausingHandler<Void> handler = new PausingHandler<>();

    OneTimeTask<Void> customTask =
        Tasks.oneTime("custom-a", Void.class)
            .onDeadExecution(new CancelDeadExecution<>())
            .execute(handler);

    TestableListener.Condition ranUpdateHeartbeats =
        TestableListener.Conditions.ranUpdateHeartbeats(3);
    TestableListener.Condition ranExecuteDue = TestableListener.Conditions.ranExecuteDue(1);

    TestableListener listener =
        TestableListener.create().waitConditions(ranUpdateHeartbeats, ranExecuteDue).build();

    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), customTask)
            .pollingInterval(Duration.ofMillis(30))
            .heartbeatInterval(Duration.ofMillis(30))
            .missedHeartbeatsLimit(10)
            .schedulerName(new SchedulerName.Fixed("test"))
            .addSchedulerListener(listener)
            .build();
    stopScheduler.register(scheduler);

    scheduler.schedule(customTask.instance("1"), Instant.now());
    scheduler.start();
    handler.waitForExecute.await();

    // fake other update that will cause heartbeat update to fail
    new JdbcRunner(postgres.getDataSource())
        .execute("update scheduled_tasks set version = version + 1", NOOP);
    ranUpdateHeartbeats.waitFor();

    List<CurrentlyExecuting> failing = scheduler.getCurrentlyExecutingWithStaleHeartbeat();
    handler.waitInExecuteUntil.countDown();

    assertThat(failing, IsCollectionWithSize.hasSize(1));
    HeartbeatState state = failing.get(0).getHeartbeatState();
    assertTrue(state.hasStaleHeartbeat());
    // update-heartbeats may have run once before this execution was picked, hence >= 2 and not == 3
    assertThat(state.getFailedHeartbeats(), greaterThanOrEqualTo(2));
  }
}
