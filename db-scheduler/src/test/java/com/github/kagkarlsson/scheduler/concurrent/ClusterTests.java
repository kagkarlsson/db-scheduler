package com.github.kagkarlsson.scheduler.concurrent;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.SchedulerName.Fixed;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks.SimpleStatsRegistry;
import com.github.kagkarlsson.scheduler.TestTasks.SleepingHandler;
import com.github.kagkarlsson.scheduler.Utils;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.ComposableTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterTests {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterTests.class);
  public static final int NUMBER_OF_THREADS = 10;

  static void testConcurrencyForPollingStrategy(
      DataSource dataSource,
      Consumer<SchedulerBuilder> schedulerCustomization,
      StopSchedulerExtension stopScheduler) {
    final List<String> ids = IntStream.range(1, 10001).mapToObj(String::valueOf).collect(toList());

    final CountDownLatch completeAllIds = new CountDownLatch(ids.size());
    final RecordResultAndStopExecutionOnComplete<Void> completed =
        new RecordResultAndStopExecutionOnComplete<>((id) -> completeAllIds.countDown());
    final Task<Void> task =
        ComposableTask.customTask("Custom", Void.class, completed, new SleepingHandler<>(0));

    final SimpleStatsRegistry stats = new SimpleStatsRegistry();
    final Scheduler scheduler1 =
        createScheduler(dataSource, "scheduler1", schedulerCustomization, task, stats);
    final Scheduler scheduler2 =
        createScheduler(dataSource, "scheduler2", schedulerCustomization, task, stats);

    stopScheduler.register(scheduler1, scheduler2);
    scheduler1.start();
    scheduler2.start();

    ids.forEach(
        id -> {
          scheduler1.schedule(task.instance(id), Instant.now());
        });

    final boolean waitSuccessful =
        asRuntimeException(() -> completeAllIds.await(30, TimeUnit.SECONDS));

    if (!waitSuccessful) {
      LOG.info(
          "Failed to execute all for 10s. ok={}, failed={}, errors={}",
          completed.ok.size(),
          completed.failed.size(),
          stats.unexpectedErrors.get());
    }
    scheduler1.stop();
    scheduler2.stop();

    assertThat(completed.failed.size(), is(0));
    assertThat(completed.ok.size(), is(ids.size()));
    assertThat("Should contain no duplicates", new HashSet<>(completed.ok).size(), is(ids.size()));
    assertThat(stats.unexpectedErrors.get(), is(0));
    assertThat(scheduler1.getCurrentlyExecuting(), hasSize(0));
    assertThat(scheduler2.getCurrentlyExecuting(), hasSize(0));
  }

  private static Boolean asRuntimeException(Callable<Boolean> callable) {
    try {
      return callable.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void testRecurring(StopSchedulerExtension stopScheduler, DataSource datasource) {
    final RecurringTask<Void> task1 =
        Tasks.recurring("task1", Schedules.fixedDelay(Duration.ofMillis(0)))
            .execute(
                (taskInstance, executionContext) -> {
                  // do nothing
                  // System.out.println(counter.incrementAndGet() + " " +
                  // Thread.currentThread().getName());
                });

    final SimpleStatsRegistry stats = new SimpleStatsRegistry();
    final Scheduler scheduler1 = createSchedulerRecurring("scheduler1", task1, stats, datasource);
    final Scheduler scheduler2 = createSchedulerRecurring("scheduler2", task1, stats, datasource);

    stopScheduler.register(scheduler1, scheduler2);
    scheduler1.start();
    scheduler2.start();

    Utils.sleep(5_000);

    scheduler1.stop();
    scheduler2.stop();
    assertThat(stats.unexpectedErrors.get(), is(0));
    assertThat(scheduler1.getCurrentlyExecuting(), hasSize(0));
    assertThat(scheduler2.getCurrentlyExecuting(), hasSize(0));
  }

  private static Scheduler createScheduler(
      DataSource datasource,
      String name,
      Consumer<SchedulerBuilder> schedulerCustomization,
      Task<?> task,
      SimpleStatsRegistry stats) {
    final SchedulerBuilder builder =
        Scheduler.create(datasource, Lists.newArrayList(task))
            .schedulerName(new Fixed(name))
            .threads(NUMBER_OF_THREADS)
            .pollingInterval(Duration.ofMillis(50)) // also runs fine with 5s
            .heartbeatInterval(Duration.ofMillis(2_000))
            .statsRegistry(stats);
    schedulerCustomization.accept(builder);
    return builder.build();
  }

  private static Scheduler createSchedulerRecurring(
      String name, RecurringTask<?> task, SimpleStatsRegistry stats, DataSource datasource) {
    return Scheduler.create(datasource)
        .startTasks(task)
        .schedulerName(new Fixed(name))
        .threads(NUMBER_OF_THREADS)
        .pollingInterval(Duration.ofMillis(50))
        .heartbeatInterval(Duration.ofMillis(2_000))
        .statsRegistry(stats)
        .build();
  }
}
