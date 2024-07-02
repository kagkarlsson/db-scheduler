package com.github.kagkarlsson.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RunUntilShutdownTest {

  private TimeLimitedRunnable runnable;
  private CountingWaiter countingWaiter;
  private RunUntilShutdown runUntilShutdown;
  private SchedulerState.SettableSchedulerState schedulerState;

  @BeforeEach
  public void setUp() {
    schedulerState = new SchedulerState.SettableSchedulerState();
    runnable = new TimeLimitedRunnable(2, schedulerState);
    countingWaiter = new CountingWaiter();
    runUntilShutdown =
        new RunUntilShutdown(runnable, countingWaiter, schedulerState, SchedulerListeners.NOOP);
  }

  @Test
  public void should_wait_on_ok_execution() {
    Assertions.assertTimeoutPreemptively(
        Duration.ofSeconds(1),
        () -> {
          runUntilShutdown.run();
          assertThat(countingWaiter.counter, is(2));
        });
  }

  @Test
  public void should_wait_on_runtime_exception() {
    Assertions.assertTimeoutPreemptively(
        Duration.ofSeconds(1),
        () -> {
          runnable.setAction(
              () -> {
                throw new RuntimeException();
              });
          runUntilShutdown.run();
          assertThat(countingWaiter.counter, is(2));
        });
  }

  private static class TimeLimitedRunnable implements Runnable {
    private int times;
    private final SchedulerState.SettableSchedulerState schedulerState;
    private Runnable action;

    public TimeLimitedRunnable(int times, SchedulerState.SettableSchedulerState schedulerState) {
      this.times = times;
      this.schedulerState = schedulerState;
    }

    public void setAction(Runnable action) {
      this.action = action;
    }

    @Override
    public void run() {
      times--;
      if (times <= 0) {
        schedulerState.setIsShuttingDown();
      }
      if (action != null) action.run();
    }
  }

  private static class CountingWaiter extends Waiter {
    public int counter = 0;

    public CountingWaiter() {
      super(Duration.ofHours(1));
    }

    @Override
    public void doWait() throws InterruptedException {
      counter++;
    }
  }
}
