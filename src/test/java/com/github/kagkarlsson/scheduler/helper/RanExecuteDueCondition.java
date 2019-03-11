package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;

import java.util.concurrent.CountDownLatch;

public class RanExecuteDueCondition implements TestableRegistry.Condition {

    private final CountDownLatch count;

    public RanExecuteDueCondition(int waitForCount) {
        count = new CountDownLatch(waitForCount);
    }

    @Override
    public void waitFor() {
        try {
            count.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void apply(StatsRegistry.SchedulerStatsEvent e) {
        if (e == StatsRegistry.SchedulerStatsEvent.RAN_EXECUTE_DUE) {
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
