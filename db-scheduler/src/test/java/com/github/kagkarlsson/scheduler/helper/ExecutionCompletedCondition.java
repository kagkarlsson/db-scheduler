package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

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
            LoggerFactory.getLogger(ExecutionCompletedCondition.class).debug("Starting await for "+numberCompleted+" ExecutionCompleted");
            completed.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void apply(StatsRegistry.SchedulerStatsEvent e) {
    }

    @Override
    public void apply(StatsRegistry.CandidateStatsEvent e) {
    }

    @Override
    public void apply(StatsRegistry.ExecutionStatsEvent e) {
        if (e == StatsRegistry.ExecutionStatsEvent.COMPLETED) {
            LoggerFactory.getLogger(ExecutionCompletedCondition.class).debug("Received event execution-completed, counting down");
            completed.countDown();
        }
    }
}
