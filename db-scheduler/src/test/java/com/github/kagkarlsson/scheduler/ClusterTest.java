package com.github.kagkarlsson.scheduler;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.helper.ComposableTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class ClusterTest {

	@Rule
	public EmbeddedPostgresqlRule DB = new EmbeddedPostgresqlRule(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);
	@Rule
	public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);
	@Rule
	public StopSchedulerRule stopScheduler = new StopSchedulerRule();

	@Test
	public void test_concurrency() throws InterruptedException {
		final List<String> ids = IntStream.range(1, 1001).mapToObj(String::valueOf).collect(toList());

		final CountDownLatch completeAllIds = new CountDownLatch(ids.size());
		final RecordResultAndStopExecutionOnComplete<Void> completed = new RecordResultAndStopExecutionOnComplete<>(
				(id) -> completeAllIds.countDown());
		final Task<Void> task = ComposableTask.customTask("Custom", Void.class, completed, new TestTasks.SleepingHandler<>(1));

		final TestTasks.SimpleStatsRegistry stats = new TestTasks.SimpleStatsRegistry();
		final Scheduler scheduler1 = createScheduler("scheduler1", task, stats);
		final Scheduler scheduler2 = createScheduler("scheduler2", task, stats);

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

	}

    @Test
    public void test_concurrency_recurring() throws InterruptedException {

        AtomicLong counter = new AtomicLong(0);
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

    }

	private Scheduler createScheduler(String name, Task<?> task, TestTasks.SimpleStatsRegistry stats) {
		return Scheduler.create(DB.getDataSource(), Lists.newArrayList(task))
				.schedulerName(new SchedulerName.Fixed(name)).pollingInterval(Duration.ofMillis(0))
				.heartbeatInterval(Duration.ofMillis(100)).statsRegistry(stats).build();
	}

    private Scheduler createSchedulerRecurring(String name, RecurringTask<?> task, TestTasks.SimpleStatsRegistry stats) {
        return Scheduler.create(DB.getDataSource())
            .startTasks(task)
            .schedulerName(new SchedulerName.Fixed(name))
            .pollingInterval(Duration.ofMillis(0))
            .heartbeatInterval(Duration.ofMillis(100))
            .statsRegistry(stats).build();
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
