package com.github.kagkarlsson.scheduler.testhelper;

import static org.testcontainers.shaded.org.hamcrest.MatcherAssert.assertThat;
import static org.testcontainers.shaded.org.hamcrest.Matchers.hasItem;

import com.github.kagkarlsson.scheduler.CurrentlyExecuting;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManualSchedulerTest {

  private static final Logger LOG = LoggerFactory.getLogger(ManualSchedulerTest.class);

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  private final SchedulerListener listener = new SchedulerListener() {

    @Override
    public void onExecutionScheduled(TaskInstanceId taskInstanceId, Instant executionTime) {
      LOG.info("onExecutionScheduled()");
    }

    @Override
    public void onExecutionStart(CurrentlyExecuting currentlyExecuting) {
      LOG.info("onExecutionStart()");
    }

    @Override
    public void onExecutionComplete(ExecutionComplete executionComplete) {
      LOG.info("onExecutionComplete()");
    }

    @Override
    public void onExecutionDead(Execution execution) {
      LOG.info("onExecutionDead()");
    }

    @Override
    public void onExecutionFailedHeartbeat(CurrentlyExecuting currentlyExecuting) {
      LOG.info("onExecutionFailedHeartbeat()");
    }

    @Override
    public void onSchedulerEvent(SchedulerEventType type) {
      LOG.info("onSchedulerEvent()");
    }

    @Override
    public void onCandidateEvent(CandidateEventType type) {
      LOG.info("onCandidateEvent()");
    }
  };

  @Test
  public void manual_scheduler_listener() {
    LOG.info("manual_scheduler_add_listener()");

    ManualScheduler scheduler = TestHelper
      .createManualScheduler(postgres.getDataSource())
      .addSchedulerListener(listener)
      .build();

    assertThat(
      scheduler.getSchedulerListeners().getSchedulerListeners(),
      hasItem(listener)
    );
  }

}
