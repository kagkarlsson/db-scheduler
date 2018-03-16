package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.FixedDelay;
import com.github.kagkarlsson.scheduler.task.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.RecurringTask;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ExecutionTest {

	@Test
	public void test_equals() {
		Instant now = Instant.now();
		OneTimeTask task = TestTasks.oneTime("OneTime", Void.class, (instance, executionContext) -> {});
		RecurringTask task2 = TestTasks.recurring("Recurring", FixedDelay.of(Duration.ofHours(1)), TestTasks.DO_NOTHING);

		assertEquals(new Execution(now, task.instance("id1")), new Execution(now, task.instance("id1")));
		assertNotEquals(new Execution(now, task.instance("id1")), new Execution(now.plus(Duration.ofMinutes(1)), task.instance("id1")));
		assertNotEquals(new Execution(now, task.instance("id1")), new Execution(now, task.instance("id2")));

		assertEquals(new Execution(now, task2.instance("id1")), new Execution(now, task2.instance("id1")));
		assertNotEquals(new Execution(now, task2.instance("id1")), new Execution(now.plus(Duration.ofMinutes(1)), task2.instance("id1")));
		assertNotEquals(new Execution(now, task2.instance("id1")), new Execution(now, task2.instance("id2")));

		assertNotEquals(new Execution(now, task.instance("id1")), new Execution(now, task2.instance("id1")));
	}
}
