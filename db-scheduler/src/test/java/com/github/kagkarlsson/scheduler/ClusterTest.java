package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.ComposableTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;


public class ClusterTest {

    public static final int NUMBER_OF_THREADS = 10;
    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @Test
    public void test_concurrency_optimistic_locking() {
        testConcurrencyForPollingStrategy(
            (SchedulerBuilder b) -> b.pollUsingFetchAndLockOnExecute(0, NUMBER_OF_THREADS*3));
    }

    @Test
    public void test_concurrency_select_for_update() {
        testConcurrencyForPollingStrategy(
            (SchedulerBuilder b) -> b.pollUsingLockAndFetch(((double)NUMBER_OF_THREADS)/2, NUMBER_OF_THREADS));
    }

    private void testConcurrencyForPollingStrategy(Consumer<SchedulerBuilder> schedulerCustomization) {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

            final List<String> ids = IntStream.range(1, 10001).mapToObj(String::valueOf).collect(toList());

            final CountDownLatch completeAllIds = new CountDownLatch(ids.size());
            final RecordResultAndStopExecutionOnComplete<Void> completed = new RecordResultAndStopExecutionOnComplete<>(
                    (id) -> completeAllIds.countDown());
            final Task<Void> task = ComposableTask.customTask("Custom", Void.class, completed, new TestTasks.SleepingHandler<>(0));

            final TestTasks.SimpleStatsRegistry stats = new TestTasks.SimpleStatsRegistry();
            final Scheduler scheduler1 = createScheduler("scheduler1", schedulerCustomization, task, stats);
            final Scheduler scheduler2 = createScheduler("scheduler2", schedulerCustomization, task, stats);

            stopScheduler.register(scheduler1, scheduler2);
            scheduler1.start();
            scheduler2.start();

            ids.forEach(id -> {
                scheduler1.schedule(task.instance(id), Instant.now());
            });

            completeAllIds.await();
            scheduler1.stop();
            scheduler2.stop();

            assertThat(completed.failed.size(), is(0));
            assertThat(completed.ok.size(), is(ids.size()));
            assertThat("Should contain no duplicates", new HashSet<>(completed.ok).size(), is(ids.size()));
            assertThat(stats.unexpectedErrors.get(), is(0));
            assertThat(scheduler1.getCurrentlyExecuting(), hasSize(0));
            assertThat(scheduler2.getCurrentlyExecuting(), hasSize(0));
        });
    }

    @Test
    public void test_concurrency_recurring() throws InterruptedException {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

            final RecurringTask<Void> task1 = Tasks.recurring("task1", Schedules.fixedDelay(Duration.ofMillis(0)))
                    .execute((taskInstance, executionContext) -> {
                        // do nothing
                        // System.out.println(counter.incrementAndGet() + " " + Thread.currentThread().getName());
                    });

            final TestTasks.SimpleStatsRegistry stats = new TestTasks.SimpleStatsRegistry();
            final Scheduler scheduler1 = createSchedulerRecurring("scheduler1", task1, stats);
            final Scheduler scheduler2 = createSchedulerRecurring("scheduler2", task1, stats);

            stopScheduler.register(scheduler1, scheduler2);
            scheduler1.start();
            scheduler2.start();

            Thread.sleep(5_000);
            scheduler1.stop();
            scheduler2.stop();
            assertThat(stats.unexpectedErrors.get(), is(0));
            assertThat(scheduler1.getCurrentlyExecuting(), hasSize(0));
            assertThat(scheduler2.getCurrentlyExecuting(), hasSize(0));
        });
    }

    private Scheduler createScheduler(String name, Consumer<SchedulerBuilder> schedulerCustomization, Task<?> task, TestTasks.SimpleStatsRegistry stats) {
        final SchedulerBuilder builder = Scheduler.create(DB.getDataSource(), Lists.newArrayList(task))
            .schedulerName(new SchedulerName.Fixed(name))
            .threads(NUMBER_OF_THREADS)
            .pollingInterval(Duration.ofMillis(0))
            .heartbeatInterval(Duration.ofMillis(100))
            .statsRegistry(stats);
        schedulerCustomization.accept(builder);
        return builder.build();
    }

    private Scheduler createSchedulerRecurring(String name, RecurringTask<?> task, TestTasks.SimpleStatsRegistry stats) {
        return Scheduler.create(DB.getDataSource())
                .startTasks(task)
                .schedulerName(new SchedulerName.Fixed(name))
                .threads(NUMBER_OF_THREADS)
                .pollingInterval(Duration.ofMillis(0))
                .heartbeatInterval(Duration.ofMillis(100))
                .statsRegistry(stats)
                .build();
    }

    private static class RecordResultAndStopExecutionOnComplete<T> implements CompletionHandler<T> {

        private final List<String> ok = Collections.synchronizedList(new ArrayList<>());
        private final List<String> failed = Collections.synchronizedList(new ArrayList<>());
        private final Consumer<String> onComplete;

        RecordResultAndStopExecutionOnComplete(Consumer<String> onComplete) {
            this.onComplete = onComplete;
        }

        @Override
        public void complete(ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
            final String instanceId = executionComplete.getExecution().taskInstance.getId();
            if (executionComplete.getResult() == ExecutionComplete.Result.OK) {
                ok.add(instanceId);
            } else {
                failed.add(instanceId);
            }
            executionOperations.stop();
            onComplete.accept(instanceId);
        }
    }
}
