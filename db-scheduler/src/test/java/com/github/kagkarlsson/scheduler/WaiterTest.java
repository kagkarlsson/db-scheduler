package com.github.kagkarlsson.scheduler;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WaiterTest {
  private ExecutorService executor;

  @BeforeEach
  public void setUp() {
    this.executor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  public void tearDown() {
    this.executor.shutdownNow();
  }

  @Test
  public void should_wait_at_least_duration() throws ExecutionException, InterruptedException {
    Future<Long> waitTime = executor.submit(new WaitForWaiter(new Waiter(Duration.ofMillis(100))));
    assertTrue(waitTime.get() >= 100L, "Waited: " + waitTime.get());
  }

  @Test
  public void should_wait_until_woken() throws ExecutionException, InterruptedException {
    Waiter waiter = new Waiter(Duration.ofMillis(1000));
    Future<Long> waitTime = executor.submit(new WaitForWaiter(waiter));
    sleep(20); // give executor time to get to wait(..)

    waiter.wake();
    assertTrue(waitTime.get() < 100L, "Waited: " + waitTime.get());

    Future<Long> waitTime2 = executor.submit(new WaitForWaiter(waiter));
    sleep(20); // give executor time to get to wait(..)

    waiter.wake();
    assertTrue(waitTime2.get() < 100L, "Waited: " + waitTime2.get());
  }

  @Test
  public void should_wait_for_duration_even_if_prematurely_notified()
      throws ExecutionException, InterruptedException {
    Object lock = new Object();

    Waiter waiter = new Waiter(Duration.ofMillis(200), new SystemClock(), lock);
    Future<Long> waitTime = executor.submit(new WaitForWaiter(waiter));
    sleep(20); // give executor time to get to wait(..)

    synchronized (lock) {
      lock.notify();
    }

    assertTrue(waitTime.get() >= 200L, "Waited: " + waitTime.get());
  }

  @Test
  public void should_not_wait_if_instructed_to_skip_next()
      throws ExecutionException, InterruptedException {
    Waiter waiter = new Waiter(Duration.ofMillis(1000));
    waiter.wakeOrSkipNextWait(); // set skip
    Future<Long> waitTime = executor.submit(new WaitForWaiter(waiter));

    assertTrue(waitTime.get() < 100L, "Waited: " + waitTime.get());
  }

  private void sleep(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
    }
  }

  private static class WaitForWaiter implements Callable<Long> {
    private Waiter waiter;

    WaitForWaiter(Waiter waiter) {
      this.waiter = waiter;
    }

    @Override
    public Long call() throws Exception {
      long start = System.currentTimeMillis();
      waiter.doWait();
      return System.currentTimeMillis() - start;
    }
  }
}
