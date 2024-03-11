package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.VoidExecutionHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.LoggerFactory;

public class TestTasks {

  public static final CompletionHandler<Void> REMOVE_ON_COMPLETE =
      new CompletionHandler.OnCompleteRemove<>();
  public static final VoidExecutionHandler<Void> DO_NOTHING =
      (taskInstance, executionContext) -> {};

  public static <T> OneTimeTask<T> oneTime(
      String name, Class<T> dataClass, VoidExecutionHandler<T> handler) {
    return new OneTimeTask<T>(name, dataClass) {
      @Override
      public void executeOnce(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
        handler.execute(taskInstance, executionContext);
      }
    };
  }

  public static <T> OneTimeTask<T> oneTimeWithType(
      String name, Class<T> dataClass, VoidExecutionHandler<T> handler) {
    return new OneTimeTask<T>(name, dataClass) {
      @Override
      public void executeOnce(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
        handler.execute(taskInstance, executionContext);
      }
    };
  }

  public static RecurringTask<Void> recurring(
      String name, FixedDelay schedule, VoidExecutionHandler<Void> handler) {
    return new RecurringTask<Void>(name, schedule, Void.class) {
      @Override
      public void executeRecurringly(
          TaskInstance<Void> taskInstance, ExecutionContext executionContext) {
        handler.execute(taskInstance, executionContext);
      }
    };
  }

  public static <T> RecurringTask<T> recurringWithData(
      String name,
      Class<T> dataClass,
      T initialData,
      FixedDelay schedule,
      VoidExecutionHandler<T> handler) {
    return new RecurringTask<T>(name, schedule, dataClass, initialData) {
      @Override
      public void executeRecurringly(
          TaskInstance<T> taskInstance, ExecutionContext executionContext) {
        handler.execute(taskInstance, executionContext);
      }
    };
  }

  public static class ResultRegisteringCompletionHandler<T> implements CompletionHandler<T> {
    final CountDownLatch waitForNotify = new CountDownLatch(1);
    ExecutionComplete.Result result;
    Optional<Throwable> cause;

    @Override
    public void complete(
        ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
      this.result = executionComplete.getResult();
      this.cause = executionComplete.getCause();
      executionOperations.stop();
      waitForNotify.countDown();
    }
  }

  public static class ResultRegisteringFailureHandler<T> implements FailureHandler<T> {
    final CountDownLatch waitForNotify = new CountDownLatch(1);
    ExecutionComplete.Result result;
    Optional<Throwable> cause;

    @Override
    public void onFailure(
        ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
      this.result = executionComplete.getResult();
      this.cause = executionComplete.getCause();
      executionOperations.stop();
      waitForNotify.countDown();
    }
  }

  public static class CountingHandler<T> implements VoidExecutionHandler<T> {
    private final Duration wait;
    public AtomicInteger timesExecuted = new AtomicInteger(0);

    public CountingHandler() {
      wait = Duration.ofMillis(0);
    }

    public CountingHandler(Duration wait) {
      this.wait = wait;
    }

    @Override
    public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
      this.timesExecuted.incrementAndGet();
      try {
        if (wait.toMillis() > 0) {
          Thread.sleep(wait.toMillis());
        }
      } catch (InterruptedException e) {
        LoggerFactory.getLogger(CountingHandler.class).info("Interrupted.");
        LoggerFactory.getLogger(CountingHandler.class).debug("Stacktrace", e);
      }
    }
  }

  public static class WaitingHandler<T> implements VoidExecutionHandler<T> {

    public final CountDownLatch waitForNotify;

    public WaitingHandler() {
      waitForNotify = new CountDownLatch(1);
    }

    @Override
    public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
      try {
        waitForNotify.await();
      } catch (InterruptedException e) {
        LoggerFactory.getLogger(WaitingHandler.class).info("Interrupted.");
      }
    }
  }

  public static class PausingHandler<T> implements VoidExecutionHandler<T> {

    public final CountDownLatch waitInExecuteUntil;
    public final CountDownLatch waitForExecute;

    public PausingHandler() {
      waitForExecute = new CountDownLatch(1);
      waitInExecuteUntil = new CountDownLatch(1);
    }

    @Override
    public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
      try {
        waitForExecute.countDown();
        LoggerFactory.getLogger(PausingHandler.class).trace("Awaiting waitInExecuteUntil.");
        waitInExecuteUntil.await();
        LoggerFactory.getLogger(PausingHandler.class)
            .trace("Received countdown for waitInExecuteUntil.");
      } catch (InterruptedException e) {
        LoggerFactory.getLogger(PausingHandler.class).info("Interrupted.");
      }
    }
  }

  public static class SleepingHandler<T> implements VoidExecutionHandler<T> {

    private final int millis;

    public SleepingHandler(int millis) {
      this.millis = millis;
    }

    @Override
    public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
      try {
        if (millis > 0) {
          Thread.sleep(millis);
        }
      } catch (InterruptedException e) {
        LoggerFactory.getLogger(WaitingHandler.class).info("Interrupted.");
      }
    }
  }

  public static class DoNothingHandler<T> implements VoidExecutionHandler<T> {

    @Override
    public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {}
  }

  public static class SavingHandler<T> implements VoidExecutionHandler<T> {
    public T savedData;

    @Override
    public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
      savedData = taskInstance.getData();
    }
  }

  public static class SimpleStatsRegistry extends StatsRegistry.DefaultStatsRegistry {
    public final AtomicInteger unexpectedErrors = new AtomicInteger(0);
    public final AtomicInteger failedHeartbeats = new AtomicInteger(0);
    public final AtomicInteger failedMultipleHeartbeats = new AtomicInteger(0);
    public final AtomicInteger deadExecutions = new AtomicInteger(0);

    @Override
    public void register(SchedulerStatsEvent e) {
      if (e == SchedulerStatsEvent.UNEXPECTED_ERROR) {
        unexpectedErrors.incrementAndGet();
      } else if (e == SchedulerStatsEvent.FAILED_HEARTBEAT) {
        failedHeartbeats.incrementAndGet();
      } else if (e == SchedulerStatsEvent.FAILED_MULTIPLE_HEARTBEATS) {
        failedMultipleHeartbeats.incrementAndGet();
      } else if (e == SchedulerStatsEvent.DEAD_EXECUTION) {
        deadExecutions.incrementAndGet();
      }
      super.register(e);
    }
  }
}
