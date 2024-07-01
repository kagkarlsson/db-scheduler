package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete.Result;
import java.util.concurrent.CountDownLatch;
import org.slf4j.LoggerFactory;

public class ExecutionCompletedCondition implements TestableRegistry.Condition {

  private final CountDownLatch completed;
  private final int numberCompleted;

  public ExecutionCompletedCondition(int numberCompleted) {
    completed = new CountDownLatch(numberCompleted);
    this.numberCompleted = numberCompleted;
  }

  @Override
  public void waitFor() {
    try {
      LoggerFactory.getLogger(ExecutionCompletedCondition.class)
          .debug("Starting await for " + numberCompleted + " ExecutionCompleted");
      completed.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void apply(StatsRegistry.SchedulerStatsEvent e) {}

  @Override
  public void apply(StatsRegistry.CandidateStatsEvent e) {}

  @Override
  public void apply(StatsRegistry.ExecutionStatsEvent e) {}

  @Override
  public void applyExecutionComplete(ExecutionComplete complete) {
    if (complete.getResult() == Result.OK) {
      LoggerFactory.getLogger(ExecutionCompletedCondition.class)
          .debug("Received event execution-completed, counting down");
      completed.countDown();
    }
  }
}
