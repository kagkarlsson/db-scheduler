package com.github.kagkarlsson.scheduler;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class WaiterTest {
    private ExecutorService executor;

    @Before
    public void setUp() {
        this.executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        this.executor.shutdownNow();
    }

    @Test
    public void should_wait_at_least_duration() throws ExecutionException, InterruptedException {
        Future<Long> waitTime = executor.submit(new WaitForWaiter(new Waiter(Duration.ofMillis(100))));
        assertTrue("Waited: " + waitTime.get(), waitTime.get() >= 100L);
    }

    @Test
    public void should_wait_until_woken() throws ExecutionException, InterruptedException {
        Waiter waiter = new Waiter(Duration.ofMillis(1000));
        Future<Long> waitTime = executor.submit(new WaitForWaiter(waiter));
        sleep(20); // give executor time to get to wait(..)

        waiter.wake();
        assertTrue("Waited: " + waitTime.get(), waitTime.get() < 100L);

        Future<Long> waitTime2 = executor.submit(new WaitForWaiter(waiter));
        sleep(20); // give executor time to get to wait(..)

        waiter.wake();
        assertTrue("Waited: " + waitTime2.get(), waitTime2.get() < 100L);
    }

    @Test
    public void should_wait_for_duration_even_if_prematurely_notified() throws ExecutionException, InterruptedException {
        Object lock = new Object();

        Waiter waiter = new Waiter(Duration.ofMillis(200), new SystemClock(), lock);
        Future<Long> waitTime = executor.submit(new WaitForWaiter(waiter));
        sleep(20); // give executor time to get to wait(..)

        synchronized (lock) {
            lock.notify();
        }

        assertTrue("Waited: " + waitTime.get(), waitTime.get() >= 200L);
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