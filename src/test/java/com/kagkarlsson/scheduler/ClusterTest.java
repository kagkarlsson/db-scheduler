package com.kagkarlsson.scheduler;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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

		final List<String> ids = IntStream.range(1, 101).mapToObj(String::valueOf).collect(toList());
		List<String> completedIds = new ArrayList<>();
		final CountDownLatch waiter = new CountDownLatch(ids.size());
		ResultRegisteringTask task = new ResultRegisteringTask("OneTime", (instance) -> {sleep(10);}, (id) -> {
			completedIds.add(id);
			waiter.countDown();
		});

		final Scheduler scheduler1 = Scheduler.create(DB.getDataSource())
				.name("scheduler1")
				.addHandler(task)
				.build();

		final Scheduler scheduler2 = Scheduler.create(DB.getDataSource())
				.name("scheduler2")
				.addHandler(task)
				.build();

		scheduler1.start();
		scheduler2.start();

		ids.forEach(id -> {
			scheduler1.schedule(LocalDateTime.now(), task.instance(id));
		});

		waiter.await();

		assertThat(completedIds.size(), is(ids.size()));
		assertThat("Should contain no duplicates", new HashSet<>(completedIds).size(), is(ids.size()));

		scheduler1.stop();
		scheduler2.stop();
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

		public ResultRegisteringTask(String name, Consumer<TaskInstance> handler, Consumer<String> onComplete) {
			super(name, handler);
			this.onComplete = onComplete;
		}

		@Override
		public void complete(Execution execution, LocalDateTime timeDone, Scheduler.TaskInstanceOperations taskInstanceOperations) {
			super.complete(execution, timeDone, taskInstanceOperations);
			onComplete.accept(execution.taskInstance.getId());
		}
	}


	private static final Consumer<TaskInstance> DO_NOTHING = (taskInstance -> {});

}
