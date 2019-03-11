package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;

import java.util.*;

import static org.junit.Assert.fail;

public class TestableRegistry implements StatsRegistry {

    public static final EnumSet<SchedulerStatsEvent> FAILURE_EVENTS = EnumSet.of(SchedulerStatsEvent.UNEXPECTED_ERROR,
            SchedulerStatsEvent.COMPLETIONHANDLER_ERROR, SchedulerStatsEvent.FAILUREHANDLER_ERROR, SchedulerStatsEvent.DEAD_EXECUTION);
    private final List<ExecutionComplete> completed;
    private final List<SchedulerStatsEvent> stats;
    private List<Condition> waitConditions;

    public TestableRegistry(List<Condition> waitConditions) {
        this.waitConditions = waitConditions;
        this.completed = Collections.synchronizedList(new ArrayList<>());
        this.stats = Collections.synchronizedList(new ArrayList<>());
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
        this.stats.add(e);
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
        completed.add(completeEvent);
    }

    public List<ExecutionComplete> getCompleted() {
        return completed;
    }

    public void assertNoFailures() {
        this.stats.stream().forEach(e -> {
            if (FAILURE_EVENTS.contains(e)) {
                fail("Statsregistry contained unexpected error: " + e);
            }
        });
    }

    public interface Condition {
        void waitFor();

        void apply(SchedulerStatsEvent e);

        void apply(CandidateStatsEvent e);

        void apply(ExecutionStatsEvent e);
    }

    public static class Builder {

        private List<Condition> waitConditions = new ArrayList<>();

        public Builder waitConditions(Condition ... waitConditions) {
            this.waitConditions.addAll(Arrays.asList(waitConditions));
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

        public static Condition ranExecuteDue(int count) {
            return new RanExecuteDueCondition(count);
        }
    }


}
