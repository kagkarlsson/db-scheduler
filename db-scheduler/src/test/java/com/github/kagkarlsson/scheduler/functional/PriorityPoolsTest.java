package com.github.kagkarlsson.scheduler.functional;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.time.Duration.ofMillis;
import static java.time.Instant.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.github.kagkarlsson.scheduler.CurrentlyExecuting;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName.Fixed;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.helper.TestableListener;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.Priority;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class PriorityPoolsTest {
  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  // Shared tracking infrastructure for pool execution counts
  private static class PoolExecutionTracker {
    private final AtomicInteger defaultPoolExecutions = new AtomicInteger(0);
    private final AtomicInteger mediumPoolExecutions = new AtomicInteger(0);
    private final AtomicInteger highPoolExecutions = new AtomicInteger(0);

    public void reset() {
      defaultPoolExecutions.set(0);
      mediumPoolExecutions.set(0);
      highPoolExecutions.set(0);
    }

    public void trackThread() {
      var threadName = Thread.currentThread().getName();
      if (threadName.contains("-p90-")) {
        highPoolExecutions.incrementAndGet();
      } else if (threadName.contains("-p50-")) {
        mediumPoolExecutions.incrementAndGet();
      } else {
        defaultPoolExecutions.incrementAndGet();
      }
    }

    public int getDefaultPoolExecutions() {
      return defaultPoolExecutions.get();
    }

    public int getMediumPoolExecutions() {
      return mediumPoolExecutions.get();
    }

    public int getHighPoolExecutions() {
      return highPoolExecutions.get();
    }

    public int getTotalExecutions() {
      return defaultPoolExecutions.get() + mediumPoolExecutions.get() + highPoolExecutions.get();
    }
  }

  private final PoolExecutionTracker tracker = new PoolExecutionTracker();

  // Reusable tracking task that records which pool executed it
  private final OneTimeTask<Void> trackingTask =
      TestTasks.oneTime(
          "tracking-task",
          Void.class,
          (instance, ctx) -> {
            tracker.trackThread();
          });

  @BeforeEach
  void setUp() {
    tracker.reset();
  }

  @Test
  public void should_route_tasks_to_correct_executor_based_on_priority() {
    TestableListener.Condition condition = TestableListener.Conditions.completed(3);
    TestableListener listener = TestableListener.create().waitConditions(condition).build();

    // Create a scheduler with multiple worker pools:
    // Default pool (threshold = Integer.MIN_VALUE)
    // Medium pool (threshold = Priority.MEDIUM = 50)
    // High pool (threshold = Priority.HIGH = 90)
    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), trackingTask)
            .threads(2) // 2 threads in default pool
            .pollingInterval(ofMillis(100))
            .schedulerName(new Fixed("test"))
            .addSchedulerListener(listener)
            .enablePriority()
            .addWorkerPool(1, Priority.MEDIUM) // 1 thread for medium+ priority
            .addWorkerPool(1, Priority.HIGH) // 1 thread for high priority only
            .build();

    stopScheduler.register(scheduler);

    // Verify scheduler configuration through currently executing method
    List<CurrentlyExecuting> allCurrently = scheduler.getCurrentlyExecuting();
    assertThat("Should initially have no running tasks", allCurrently, hasSize(0));

    // Schedule tasks with different priorities
    scheduler.schedule(trackingTask.instanceBuilder("high").priority(Priority.HIGH).build(), now());
    scheduler.schedule(
        trackingTask.instanceBuilder("medium").priority(Priority.MEDIUM).build(), now());
    scheduler.schedule(trackingTask.instanceBuilder("low").priority(Priority.LOW).build(), now());

    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          scheduler.start();
          condition.waitFor();

          List<ExecutionComplete> completed = listener.getCompleted();
          assertThat(completed, hasSize(3));
          listener.assertNoFailures();

          // Verify task routing: lower priority tasks should use default pool,
          // while higher priority tasks can use any eligible pool
          assertThat(
              "Default pool should handle one execution",
              tracker.getDefaultPoolExecutions(),
              equalTo(1));
          assertThat(
              "Medium pool should handle one execution",
              tracker.getMediumPoolExecutions(),
              equalTo(1));
          assertThat(
              "High pool should handle one execution", tracker.getHighPoolExecutions(), equalTo(1));

          // Total executions should equal the number of tasks
          assertThat("All tasks should be executed", tracker.getTotalExecutions(), equalTo(3));
        });
  }

  @Test
  public void should_execute_high_priority_tasks_when_default_pool_blocked() {
    CountDownLatch blockDefaultPool = new CountDownLatch(1);
    CountDownLatch defaultTaskStarted = new CountDownLatch(1);
    CountDownLatch highPriorityCompleted = new CountDownLatch(1);

    // Task that blocks the default pool
    OneTimeTask<Void> blockingTask =
        TestTasks.oneTime(
            "blocking-task",
            Void.class,
            (instance, ctx) -> {
              try {
                defaultTaskStarted.countDown();
                // Block the default pool thread
                blockDefaultPool.await(10, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    // High-priority task that should execute quickly
    OneTimeTask<Void> highPriorityTask =
        TestTasks.oneTime(
            "high-priority-task",
            Void.class,
            (instance, ctx) -> {
              highPriorityCompleted.countDown();
            });

    TestableListener.Condition condition = TestableListener.Conditions.completed(2);
    TestableListener listener = TestableListener.create().waitConditions(condition).build();

    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), blockingTask, highPriorityTask)
            .threads(1) // Only 1 thread in default pool
            .pollingInterval(ofMillis(100))
            .schedulerName(new Fixed("test"))
            .addSchedulerListener(listener)
            .enablePriority()
            .addWorkerPool(1, Priority.HIGH) // 1 dedicated high-priority thread
            .build();

    stopScheduler.register(scheduler);

    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          scheduler.start();

          // Schedule a low-priority task that will block the default pool
          scheduler.schedule(
              blockingTask.instanceBuilder("blocker").priority(Priority.LOW).build(), now());

          // Wait for the blocking task to start
          defaultTaskStarted.await(5, TimeUnit.SECONDS);

          // Now schedule a high-priority task - it should execute immediately on the high-priority
          // pool
          long startTime = System.currentTimeMillis();
          scheduler.schedule(
              highPriorityTask.instanceBuilder("high").priority(Priority.HIGH).build(), now());

          // High-priority task should complete quickly even though default pool is blocked
          boolean completed = highPriorityCompleted.await(3, TimeUnit.SECONDS);
          long endTime = System.currentTimeMillis();
          long executionTime = endTime - startTime;

          assertThat("High priority task should complete within timeout", completed, equalTo(true));
          assertThat("High priority task should execute quickly", executionTime, lessThan(2000L));

          // Release the blocking task
          blockDefaultPool.countDown();

          // Wait for all tasks to complete
          condition.waitFor();

          listener.assertNoFailures();
        });
  }

  @Test
  public void should_route_eligible_tasks_to_medium_pool_and_low_to_default() {
    TestableListener.Condition condition = TestableListener.Conditions.completed(3);
    TestableListener listener = TestableListener.create().waitConditions(condition).build();

    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), trackingTask)
            .threads(1) // Default pool: 1 thread
            .pollingInterval(ofMillis(100))
            .schedulerName(new Fixed("test"))
            .addSchedulerListener(listener)
            .enablePriority()
            .addWorkerPool(1, Priority.MEDIUM) // Medium pool: 1 thread, threshold=50
            .build();

    stopScheduler.register(scheduler);

    // High and medium priority tasks should use medium pool (both >= 50)
    // Low priority task should use default pool (< 50)
    scheduler.schedule(trackingTask.instanceBuilder("high").priority(Priority.HIGH).build(), now());
    scheduler.schedule(
        trackingTask.instanceBuilder("medium").priority(Priority.MEDIUM).build(), now());
    scheduler.schedule(trackingTask.instanceBuilder("low").priority(Priority.LOW).build(), now());

    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          scheduler.start();
          condition.waitFor();

          List<ExecutionComplete> completed = listener.getCompleted();
          assertThat(completed, hasSize(3));
          listener.assertNoFailures();

          // Verify proper routing based on medium pool threshold (50)
          // High-priority (90) and medium-priority (50) tasks should both be eligible for medium
          // pool
          // Low-priority (10) task should only be eligible for default pool
          assertThat(
              "Medium and high priority tasks should execute on medium pool (at least 1)",
              tracker.getMediumPoolExecutions(),
              greaterThanOrEqualTo(1));
          assertThat(
              "Low-priority task should execute on default pool (at least 1)",
              tracker.getDefaultPoolExecutions(),
              greaterThanOrEqualTo(1));
          assertThat("All tasks should be accounted for", tracker.getTotalExecutions(), equalTo(3));
        });
  }

  @Test
  public void should_fallback_to_lower_priority_pools_when_higher_pools_are_full() {
    CountDownLatch blockHighPool = new CountDownLatch(1);
    CountDownLatch highPoolTaskStarted = new CountDownLatch(1);
    CountDownLatch fallbackTaskCompleted = new CountDownLatch(1);

    // Task that will block the high-priority pool
    OneTimeTask<Void> blockingTask =
        TestTasks.oneTime(
            "blocking-task",
            Void.class,
            (instance, ctx) -> {
              try {
                highPoolTaskStarted.countDown();
                // Block the high-priority pool thread
                blockHighPool.await(15, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    // High-priority task that should fallback to medium pool when high pool is full
    OneTimeTask<Void> fallbackTask =
        TestTasks.oneTime(
            "fallback-task",
            Void.class,
            (instance, ctx) -> {
              // Track which pool this task executed on
              String threadName = Thread.currentThread().getName();

              // This task should execute on medium pool (-p50-) since high pool is blocked
              if (threadName.contains("-p50-")) {
                fallbackTaskCompleted.countDown();
              } else {
                throw new RuntimeException(
                    "High-priority task should have fallen back to medium pool, but executed on: "
                        + threadName);
              }
            });

    TestableListener.Condition condition = TestableListener.Conditions.completed(2);
    TestableListener listener = TestableListener.create().waitConditions(condition).build();

    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), blockingTask, fallbackTask)
            .threads(2) // Default pool: 2 threads
            .pollingInterval(ofMillis(50))
            .schedulerName(new Fixed("test"))
            .addSchedulerListener(listener)
            .enablePriority()
            .addWorkerPool(1, Priority.MEDIUM) // Medium pool: 1 thread, threshold=50
            .addWorkerPool(1, Priority.HIGH) // High pool: 1 thread, threshold=90
            .build();

    stopScheduler.register(scheduler);

    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          scheduler.start();

          // 1. Schedule a high-priority task that will block the high-priority pool
          scheduler.schedule(
              blockingTask.instanceBuilder("blocker").priority(Priority.HIGH).build(), now());

          // Wait for the blocking task to start and occupy the high-priority pool
          boolean highPoolBlocked = highPoolTaskStarted.await(3, TimeUnit.SECONDS);
          assertThat("High pool blocking task should start", highPoolBlocked, equalTo(true));

          // Give it a moment to ensure the pool is really blocked
          Thread.sleep(200);

          // 2. Schedule another high-priority task - it should fallback to medium pool since high
          // pool is full
          long startTime = System.currentTimeMillis();
          scheduler.schedule(
              fallbackTask.instanceBuilder("fallback").priority(Priority.HIGH).build(), now());

          // 3. The fallback task should execute quickly on the medium pool
          boolean fallbackCompleted = fallbackTaskCompleted.await(5, TimeUnit.SECONDS);
          long endTime = System.currentTimeMillis();
          long executionTime = endTime - startTime;

          assertThat(
              "High-priority task should fallback to medium pool when high pool is full",
              fallbackCompleted,
              equalTo(true));
          assertThat(
              "Fallback execution should be quick (< 2 seconds)", executionTime, lessThan(2000L));

          // Clean up: unblock the high pool
          blockHighPool.countDown();

          // Wait for all tasks to complete
          condition.waitFor();
          listener.assertNoFailures();
        });
  }

  @Test
  public void should_not_fill_beyond_upper_limit_with_LOCK_AND_FETCH_strategy() {
    var defaultPoolSize = 10;
    var highPoolSize = 5;
    var lowPriTasksToSchedule = 20;
    var highPriTasksToSchedule = 1;
    var lowerLimit = 0.5;
    var upperLimit = 1.0;
    var initialTasksCount = min(defaultPoolSize, lowPriTasksToSchedule) + min(highPoolSize, highPriTasksToSchedule);
    var allTasksStarted = new CountDownLatch(initialTasksCount);
    var allowTasksToComplete = new CountDownLatch(1);

    OneTimeTask<Void> workTask =
      TestTasks.oneTime(
        "work-task",
        Void.class,
        (instance, ctx) -> {
          try {
            tracker.trackThread();
            allTasksStarted.countDown();
            // Hold the thread until we allow completion
            allowTasksToComplete.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });

    var condition = TestableListener.Conditions.completed(lowPriTasksToSchedule + highPriTasksToSchedule);
    var listener = TestableListener.create().waitConditions(condition).build();

    var scheduler =
      Scheduler.create(postgres.getDataSource(), workTask)
        .threads(defaultPoolSize)
        .pollingInterval(ofMillis(50))
        .schedulerName(new Fixed("test"))
        .addSchedulerListener(listener)
        .enablePriority()
        .addWorkerPool(highPoolSize, Priority.HIGH)
        .pollUsingLockAndFetch(lowerLimit, upperLimit)
        .build();

    stopScheduler.register(scheduler);

    assertTimeoutPreemptively(
      Duration.ofSeconds(10),
      () -> {
        // Schedule low-priority tasks
        for (int i = 0; i < lowPriTasksToSchedule; i++) {
          scheduler.schedule(
            workTask.instanceBuilder("low-" + i).priority(Priority.LOW).build(), now());
        }

        // Schedule high-priority tasks
        for (int i = 0; i < highPriTasksToSchedule; i++) {
          scheduler.schedule(
            workTask.instanceBuilder("high-" + i).priority(Priority.HIGH).build(), now());
        }
        scheduler.start();

        // Wait for all tasks to be processed
        boolean allStarted = allTasksStarted.await(5, TimeUnit.SECONDS);

        assertThat("All tasks should be executing", allStarted, equalTo(true));

        Thread.sleep(2000);

        assertThat("No more than " + initialTasksCount + " tasks should be locked", scheduler.getQueuedExecutions(), equalTo(initialTasksCount));

        // Allow tasks to complete
        allowTasksToComplete.countDown();

        // Wait for all tasks to finish
        condition.waitFor();

        listener.assertNoFailures();
      });
  }

  @Test
  public void should_utilize_all_pools_with_LOCK_AND_FETCH_strategy() {
    // This test verifies that with the fix, all thread pools are properly utilized
    // when there are sufficient high-priority tasks available
    CountDownLatch allTasksStarted = new CountDownLatch(15);
    CountDownLatch allowTasksToComplete = new CountDownLatch(1);

    // Task that blocks until we allow it to complete, simulating work
    OneTimeTask<Void> workTask =
        TestTasks.oneTime(
            "work-task",
            Void.class,
            (instance, ctx) -> {
              try {
                tracker.trackThread();
                allTasksStarted.countDown();
                // Hold the thread until we allow completion
                allowTasksToComplete.await(10, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    TestableListener.Condition condition = TestableListener.Conditions.completed(20);
    TestableListener listener = TestableListener.create().waitConditions(condition).build();

    // Configuration: 10 default + 5 high = 15 total threads
    // Using LOCK_AND_FETCH strategy which is more sensitive to the limits issue
    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), workTask)
            .threads(10) // 10 threads in default pool
            .pollingInterval(ofMillis(50))
            .schedulerName(new Fixed("test"))
            .addSchedulerListener(listener)
            .enablePriority()
            .addWorkerPool(5, Priority.HIGH) // 5 threads for high-priority
            .pollUsingLockAndFetch(0.5, 1.0) // LOCK_AND_FETCH strategy
            .build();

    stopScheduler.register(scheduler);

    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          // Schedule 20 tasks: 10 high-priority and 10 low-priority
          for (int i = 0; i < 10; i++) {
            scheduler.schedule(
                workTask.instanceBuilder("high-" + i).priority(Priority.HIGH).build(), now());
            scheduler.schedule(
                workTask.instanceBuilder("low-" + i).priority(Priority.LOW).build(), now());
          }

          scheduler.start();

          // Wait for all 15 threads to be busy (may take a moment for polling)
          boolean allStarted = allTasksStarted.await(5, TimeUnit.SECONDS);
          assertThat("All 15 threads should be executing tasks", allStarted, equalTo(true));

          // Verify the distribution:
          // - High-priority pool should be full: 5 high-priority tasks
          // - Default pool should be full: 10 tasks (5 high + 5 low)
          // Note: High-priority tasks can execute on either pool, but low can only use default

          assertThat(
              "High pool should execute 5 tasks",
              tracker.getHighPoolExecutions(),
              greaterThanOrEqualTo(5));
          assertThat(
              "Default pool should execute at least 5 tasks (the low-priority ones)",
              tracker.getDefaultPoolExecutions(),
              greaterThanOrEqualTo(5));
          assertThat(
              "Total of 15 threads should be utilized", tracker.getTotalExecutions(), equalTo(15));

          // Allow tasks to complete
          allowTasksToComplete.countDown();

          // Wait for all tasks to finish
          condition.waitFor();
          listener.assertNoFailures();

          // Final verification: all 20 tasks should have executed
          assertThat(
              "All 20 tasks should have been executed", tracker.getTotalExecutions(), equalTo(20));
        });
  }

  @Test
  public void high_priority_pool_starves_when_fetch_returns_only_low_priority_tasks() {
    // This test reproduces the starvation issue with global upper limits in LOCK_AND_FETCH:
    //
    // Setup:
    //   - Default pool: 2 threads
    //   - High-priority pool: 1 thread
    //   - Total: 3 threads, upperLimit = 6 (with 2.0 fraction)
    //   - Using LOCK_AND_FETCH which calculates: executionsToFetch = upperLimit -
    // totalInQueueOrProcessing
    //
    // Scenario:
    //   1. Schedule many low-priority blocking tasks
    //   2. Fetch locks and queues tasks up to upperLimit (6 tasks)
    //   3. All 6 slots consumed by low-priority tasks (2 executing + 4 queued in default pool)
    //   4. executionsToFetch = upperLimit - totalInQueueOrProcessing = 6 - 6 = 0
    //   5. High-priority task arrives but cannot be fetched
    //   6. High-priority pool sits idle while high-priority task starves
    //
    // Expected behavior (with per-pool upper limits fix):
    //   - High-priority pool has its own capacity budget
    //   - Even when default pool queue is full, high-priority pool can still fetch
    //   - High-priority task executes promptly

    CountDownLatch defaultPoolBusy = new CountDownLatch(2);
    CountDownLatch allowLowPriorityToComplete = new CountDownLatch(1);
    CountDownLatch highPriorityStarted = new CountDownLatch(1);

    OneTimeTask<Void> blockingLowPriorityTask =
        TestTasks.oneTime(
            "blocking-low-priority",
            Void.class,
            (instance, ctx) -> {
              try {
                defaultPoolBusy.countDown();
                allowLowPriorityToComplete.await(10, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    OneTimeTask<Void> highPriorityTask =
        TestTasks.oneTime(
            "high-priority-task",
            Void.class,
            (instance, ctx) -> {
              highPriorityStarted.countDown();
            });

    TestableListener listener = TestableListener.create().build();

    // Use LOCK_AND_FETCH strategy where the starvation issue occurs
    // executionsToFetch = upperLimit - totalInQueueOrProcessing
    // With 3 threads * 2.0 = 6 upper limit
    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), blockingLowPriorityTask, highPriorityTask)
            .threads(2)
            .pollingInterval(ofMillis(50))
            .schedulerName(new Fixed("test"))
            .addSchedulerListener(listener)
            .enablePriority()
            .addWorkerPool(1, Priority.HIGH)
            .pollUsingLockAndFetch(0.5, 2.0) // LOCK_AND_FETCH with upperLimit = 6
            .build();

    stopScheduler.register(scheduler);

    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          // Schedule enough low-priority tasks to fill the global upperLimit
          // These will be locked and queued, consuming the budget
          for (int i = 0; i < 10; i++) {
            scheduler.schedule(
                blockingLowPriorityTask.instanceBuilder("low-" + i).priority(Priority.LOW).build(),
                now());
          }

          scheduler.start();

          // Wait for default pool threads to be executing
          boolean defaultBusy = defaultPoolBusy.await(3, TimeUnit.SECONDS);
          assertThat("Default pool threads should be executing", defaultBusy, equalTo(true));

          // Allow time for LOCK_AND_FETCH to lock more tasks up to upperLimit
          // This fills totalInQueueOrProcessing to the point where executionsToFetch = 0
          Thread.sleep(300);

          // NOW schedule a high-priority task - this is where starvation occurs
          // With global upperLimit: executionsToFetch = 0 → no fetch happens
          // With per-pool limits: high-priority pool has capacity → fetch happens
          scheduler.schedule(
              highPriorityTask.instanceBuilder("urgent").priority(Priority.HIGH).build(), now());

          // The high-priority task should execute promptly on the high-priority pool
          // even though the default pool queue is full of low-priority tasks
          boolean highStarted = highPriorityStarted.await(2, TimeUnit.SECONDS);

          // Release blocking tasks before assertion to speed up cleanup
          allowLowPriorityToComplete.countDown();

          assertThat(
              "High-priority task should execute even when low-priority queue is at capacity. "
                  + "This requires per-pool upper limits instead of global upper limit.",
              highStarted,
              equalTo(true));
        });
  }
}
