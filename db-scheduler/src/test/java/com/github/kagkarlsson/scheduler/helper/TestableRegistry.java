package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestableRegistry implements StatsRegistry {

  public static final EnumSet<SchedulerStatsEvent> FAILURE_EVENTS =
      EnumSet.of(
          SchedulerStatsEvent.UNEXPECTED_ERROR,
          SchedulerStatsEvent.COMPLETIONHANDLER_ERROR,
          SchedulerStatsEvent.FAILUREHANDLER_ERROR,
          SchedulerStatsEvent.DEAD_EXECUTION);

  private static final Logger REGISTRY_LOGGER = LoggerFactory.getLogger(TestableRegistry.class);

  private final List<ExecutionComplete> completed;
  private final List<SchedulerStatsEvent> stats;
  private List<Condition> waitConditions;
  private Map<String, AtomicLong> counters = new ConcurrentHashMap();
  private final boolean logEvents;

  public TestableRegistry(boolean logEvents, List<Condition> waitConditions) {
    this.logEvents = logEvents;
    this.waitConditions = Collections.synchronizedList(waitConditions);
    this.completed = Collections.synchronizedList(new ArrayList<>());
    this.stats = Collections.synchronizedList(new ArrayList<>());
  }

  public static TestableRegistry.Builder create() {
    return new TestableRegistry.Builder();
  }

  @Override
  public void register(SchedulerStatsEvent e) {
    this.stats.add(e);
    applyToConditions(e);
    log(e);
    countEvent(e);
  }

  @Override
  public void register(CandidateStatsEvent e) {
    applyToConditions(e);
    log(e);
    countEvent(e);
  }

  @Override
  public void register(ExecutionStatsEvent e) {
    applyToConditions(e);
    log(e);
    countEvent(e);
  }

  @Override
  public void registerSingleCompletedExecution(ExecutionComplete completeEvent) {
    completed.add(completeEvent);
    applyToConditions(completeEvent);
    log(completeEvent);
  }

  public List<ExecutionComplete> getCompleted() {
    return completed;
  }

  public long getCount(Enum e) {
    AtomicLong count = counters.get(counterKey(e.getClass(), e.name()));
    return count != null ? count.get() : 0;
  }

  public void assertNoFailures() {
    this.stats.forEach(
        e -> {
          if (FAILURE_EVENTS.contains(e)) {
            Assertions.fail("Statsregistry contained unexpected error: " + e);
          }
        });
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

  private void applyToConditions(ExecutionComplete complete) {
    waitConditions.forEach(c -> c.applyExecutionComplete(complete));
  }


  private void countEvent(Enum e) {
    String key = counterKey(e.getClass(), e.name());
    AtomicLong counter = counters.get(key);
    if (counter == null) {
      synchronized (counters) {
        if (counters.get(key) == null) {
          counter = new AtomicLong(0);
          counters.put(key, counter);
        } else {
          counter = counters.get(key);
        }
      }
    }

    counter.incrementAndGet();
  }

  private String counterKey(Class enumClass, String enumName) {
    return enumClass.getName() + "." + enumName;
  }

  private void log(SchedulerStatsEvent e) {
    log("Event: " + e.name());
  }

  private void log(CandidateStatsEvent e) {
    log("Event: " + e.name());
  }

  private void log(ExecutionStatsEvent e) {
    log("Event: " + e.name());
  }

  private void log(ExecutionComplete completeEvent) {
    log("Event execution complete: " + completeEvent.getExecution().toString());
  }

  private void log(String s) {
    if (logEvents) {
      REGISTRY_LOGGER.info(s);
    }
  }

  public interface Condition {
    void waitFor();

    void apply(SchedulerStatsEvent e);

    void apply(CandidateStatsEvent e);

    void apply(ExecutionStatsEvent e);

    void applyExecutionComplete(ExecutionComplete complete);
  }

  public static class Builder {

    private List<Condition> waitConditions = new ArrayList<>();
    private boolean logEvents = false;

    public Builder waitConditions(Condition... waitConditions) {
      this.waitConditions.addAll(Arrays.asList(waitConditions));
      return this;
    }

    public Builder logEvents() {
      this.logEvents = true;
      return this;
    }

    public TestableRegistry build() {
      return new TestableRegistry(logEvents, waitConditions);
    }
  }

  public static class Conditions {
    public static Condition completed(int numberCompleted) {
      return new ExecutionCompletedCondition(numberCompleted);
    }

    public static Condition ranUpdateHeartbeats(int count) {
      return new RanUpdateHeartbeatsCondition(count);
    }

    public static Condition ranExecuteDue(int count) {
      return new RanExecuteDueCondition(count);
    }
  }
}
