package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.CurrentlyExecuting;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestableListener implements SchedulerListener {

  public static final EnumSet<SchedulerEventType> FAILURE_EVENTS =
      EnumSet.of(
          SchedulerEventType.UNEXPECTED_ERROR,
          SchedulerEventType.COMPLETIONHANDLER_ERROR,
          SchedulerEventType.FAILUREHANDLER_ERROR,
          SchedulerEventType.DEAD_EXECUTION);

  private static final Logger LISTENER_LOGGER = LoggerFactory.getLogger(TestableListener.class);

  private final List<ExecutionComplete> completed;
  private final List<SchedulerEventType> stats;
  private List<Condition> waitConditions;
  private Map<String, AtomicLong> counters = new ConcurrentHashMap();
  private final boolean logEvents;

  public TestableListener(boolean logEvents, List<Condition> waitConditions) {
    this.logEvents = logEvents;
    this.waitConditions = Collections.synchronizedList(waitConditions);
    this.completed = Collections.synchronizedList(new ArrayList<>());
    this.stats = Collections.synchronizedList(new ArrayList<>());
  }

  public static TestableListener.Builder create() {
    return new TestableListener.Builder();
  }

  @Override
  public void onExecutionScheduled(TaskInstanceId taskInstanceId, Instant executionTime) {
    log(taskInstanceId, executionTime);
  }

  @Override
  public void onExecutionStart(CurrentlyExecuting currentlyExecuting) {
    logOnExecutionStart(currentlyExecuting);
  }

  @Override
  public void onExecutionComplete(ExecutionComplete executionComplete) {
    completed.add(executionComplete);
    applyToConditions(executionComplete);
    log(executionComplete);
  }

  @Override
  public void onExecutionDead(Execution execution) {
    log(execution);
  }

  @Override
  public void onExecutionFailedHeartbeat(CurrentlyExecuting currentlyExecuting) {
    logOnExecutionFailedHeartbeat(currentlyExecuting);
  }

  @Override
  public void onSchedulerEvent(SchedulerEventType type) {
    this.stats.add(type);
    applyToConditions(type);
    log(type);
    countEvent(type);
  }

  @Override
  public void onCandidateEvent(CandidateEventType type) {
    applyToConditions(type);
    log(type);
    countEvent(type);
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

  private void applyToConditions(SchedulerEventType e) {
    waitConditions.forEach(c -> c.apply(e));
  }

  private void applyToConditions(CandidateEventType e) {
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

  public void log(TaskInstanceId taskInstanceId, Instant executionTime) {
    log("Execution scheduled: " + taskInstanceId + " at " + executionTime);
  }

  public void logOnExecutionStart(CurrentlyExecuting currentlyExecuting) {
    log("Execution started: " + currentlyExecuting);
  }

  public void log(Execution execution) {
    log("Execution died: " + execution);
  }

  public void logOnExecutionFailedHeartbeat(CurrentlyExecuting currentlyExecuting) {
    log("Execution failed heartbeat: " + currentlyExecuting);
  }

  private void log(SchedulerEventType e) {
    log("Event: " + e.name());
  }

  private void log(CandidateEventType e) {
    log("Event: " + e.name());
  }

  private void log(ExecutionComplete completeEvent) {
    log("Event execution complete: " + completeEvent.getExecution().toString());
  }

  private void log(String s) {
    if (logEvents) {
      LISTENER_LOGGER.info(s);
    }
  }

  public interface Condition {
    void waitFor();

    void apply(SchedulerEventType e);

    void apply(CandidateEventType e);

    void applyExecutionComplete(ExecutionComplete complete);
  }

  public static class Builder {

    private final List<Condition> waitConditions = new ArrayList<>();
    private boolean logEvents = false;

    public TestableListener.Builder waitConditions(Condition... waitConditions) {
      this.waitConditions.addAll(Arrays.asList(waitConditions));
      return this;
    }

    public TestableListener.Builder logEvents() {
      this.logEvents = true;
      return this;
    }

    public TestableListener build() {
      return new TestableListener(logEvents, waitConditions);
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
