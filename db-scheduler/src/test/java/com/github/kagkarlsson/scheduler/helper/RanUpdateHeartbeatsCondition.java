package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RanUpdateHeartbeatsCondition implements TestableRegistry.Condition {
  private static final Logger LOG = LoggerFactory.getLogger(RanUpdateHeartbeatsCondition.class);

  private final CountDownLatch count;
  private final int waitForCount;

  public RanUpdateHeartbeatsCondition(int waitForCount) {
    count = new CountDownLatch(waitForCount);
    this.waitForCount = waitForCount;
  }

  @Override
  public void waitFor() {
    try {
      LOG.debug("Starting await for " + waitForCount + " UpdateHeartbeats");
      count.await();
      LOG.debug("Finished wait for " + waitForCount + " UpdateHeartbeats");
    } catch (InterruptedException e) {
      LOG.debug("Interrupted");
    }
  }

  @Override
  public void apply(StatsRegistry.SchedulerStatsEvent e) {
    if (e == SchedulerStatsEvent.RAN_UPDATE_HEARTBEATS) {
      LOG.info("Received event update-heartbeats, counting down");
      count.countDown();
    }
  }

  @Override
  public void apply(StatsRegistry.CandidateStatsEvent e) {}

  @Override
  public void apply(StatsRegistry.ExecutionStatsEvent e) {}

  @Override
  public void applyExecutionComplete(ExecutionComplete complete) {}
}
