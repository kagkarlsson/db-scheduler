package com.kagkarlsson.scheduler;

import com.kagkarlsson.scheduler.task.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ClusterTest {

	@Rule
	public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();

	@Before
	public void setUp() {
	}

	@Test
	public void test_concurrency() throws InterruptedException {

		final List<String> ids = IntStream.range(1, 1001).mapToObj(String::valueOf).collect(toList());
		List<String> completedIds = new ArrayList<>();
		final CountDownLatch waiter = new CountDownLatch(ids.size());
		ResultRegisteringTask task = new ResultRegisteringTask("OneTime", (instance) -> {sleep(1);}, (id) -> {
			completedIds.add(id);
			waiter.countDown();
		});

		final Scheduler scheduler1 = createScheduler("scheduler1", task);
		final Scheduler scheduler2 = createScheduler("scheduler2", task);

		scheduler1.start();
		scheduler2.start();

		ids.forEach(id -> {
			scheduler1.addExecution(LocalDateTime.now(), task.instance(id));
		});

		waiter.await();

		assertThat(completedIds.size(), is(ids.size()));
		assertThat("Should contain no duplicates", new HashSet<>(completedIds).size(), is(ids.size()));

		scheduler1.stop();
		scheduler2.stop();
	}

	private Scheduler createScheduler(String name, ResultRegisteringTask task) {
		return Scheduler.create(DB.getDataSource())
				.name(name)
				.pollingInterval(0, TimeUnit.MILLISECONDS)
				.addTask(task)
				.build();
	}

	private void sleep(int millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static class ResultRegisteringTask extends OneTimeTask {

		private final Consumer<String> onComplete;

		public ResultRegisteringTask(String name, ExecutionHandler executionHandler, Consumer<String> onComplete) {
			super(name, executionHandler);
			this.onComplete = onComplete;
		}

		@Override
		public void complete(ExecutionComplete executionComplete, Scheduler.ExecutionFinishedOperations executionFinishedOperations) {
			super.complete(executionComplete, executionFinishedOperations);
			onComplete.accept(executionComplete.getExecution().taskInstance.getId());
		}
	}


	public static final Consumer<TaskInstance> DO_NOTHING = (taskInstance -> {});

}
