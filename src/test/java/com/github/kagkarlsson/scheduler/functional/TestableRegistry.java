package com.github.kagkarlsson.scheduler.functional;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;

import java.util.ArrayList;
import java.util.List;

public class TestableRegistry implements StatsRegistry {

    private List<Condition> waitConditions;

    public TestableRegistry(List<Condition> waitConditions) {
        this.waitConditions = waitConditions;
    }

    public static TestableRegistry.Builder create() {
        return new TestableRegistry.Builder();
    }

    private void applyToConditions(SchedulerStatsEvent e) {
        waitConditions.forEach(c -> c.apply(e));
    }

    private void applyToConditions(CandidateStatsEvent e) {
        waitConditions.forEach(c -> c.apply(e));
    }

    private void applyToConditions(ExecutionStatsEvent e) {
        waitConditions.forEach(c -> c.apply(e));
    }

    @Override
    public void register(SchedulerStatsEvent e) {
        applyToConditions(e);
    }

    @Override
    public void register(CandidateStatsEvent e) {
        applyToConditions(e);
    }

    @Override
    public void register(ExecutionStatsEvent e) {
        applyToConditions(e);
    }


    @Override
    public void registerSingleCompletedExecution(ExecutionComplete completeEvent) {
    }

    public static class Builder {

        private List<Condition> waitConditions = new ArrayList<>();

        public Builder waitCondition(Condition waitCondition) {
            waitConditions.add(waitCondition);
            return this;
        }

        public TestableRegistry build() {
            return new TestableRegistry(waitConditions);
        }
    }

    public static class Conditions {
        public static Condition completed(int numberCompleted) {
            return new ExecutionCompletedCondition(numberCompleted);
        }
    }

    public interface Condition {
        void waitFor();
        void apply(SchedulerStatsEvent e);
        void apply(CandidateStatsEvent e);
        void apply(ExecutionStatsEvent e);
    }


}
