package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import java.util.concurrent.CountDownLatch;
import org.slf4j.LoggerFactory;

public class RanExecuteDueCondition implements TestableRegistry.Condition {

    private final CountDownLatch count;
    private final int waitForCount;

    public RanExecuteDueCondition(int waitForCount) {
        count = new CountDownLatch(waitForCount);
        this.waitForCount = waitForCount;
    }

    @Override
    public void waitFor() {
        try {
            LoggerFactory.getLogger(RanExecuteDueCondition.class)
                    .info("Starting await for " + waitForCount + " ExecutionCompleted");
            count.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void apply(StatsRegistry.SchedulerStatsEvent e) {
        if (e == StatsRegistry.SchedulerStatsEvent.RAN_EXECUTE_DUE) {
            LoggerFactory.getLogger(RanExecuteDueCondition.class).info("Received event executed-due, counting down");
            count.countDown();
        }
    }

    @Override
    public void apply(StatsRegistry.CandidateStatsEvent e) {
    }

    @Override
    public void apply(StatsRegistry.ExecutionStatsEvent e) {
    }
}
