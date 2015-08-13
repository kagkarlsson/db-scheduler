package com.kagkarlsson.scheduler;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.*;

public class InMemoryRepositoryTest {

	private InMemoryTaskRespository taskRespository;
	private OneTimeTask oneTimeTask;
	private RecurringTask recurringTask;

	@Before
	public void setUp() {
		taskRespository = new InMemoryTaskRespository();
		oneTimeTask = new OneTimeTask("OneTime", instance -> {});
		recurringTask = new RecurringTask("RecurringTask", Duration.ofSeconds(1), instance -> {});
	}

	@Test
	public void test_createIfNotExists() {
		LocalDateTime now = LocalDateTime.now();

		TaskInstance instance1 = oneTimeTask.instance("id1");
		TaskInstance instance2 = oneTimeTask.instance("id2");

		assertTrue(taskRespository.createIfNotExists(new Execution(now, instance1)));
		assertFalse(taskRespository.createIfNotExists(new Execution(now, instance1)));

		assertTrue(taskRespository.createIfNotExists(new Execution(now, instance2)));

		TaskInstance recurring1 = recurringTask.instance("id1");

		assertTrue(taskRespository.createIfNotExists(new Execution(now, recurring1)));
		assertFalse(taskRespository.createIfNotExists(new Execution(now, recurring1)));
	}

	@Test
	public void get_due_should_only_include_due_executions() {
		LocalDateTime now = LocalDateTime.now();

		taskRespository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
		assertThat(taskRespository.getDue(now), hasSize(1));
		assertThat(taskRespository.getDue(now.minusSeconds(1)), hasSize(0));
	}

	@Test
	public void get_due_should_be_sorted() {
		LocalDateTime now = LocalDateTime.now();
		IntStream.range(0, 100).forEach(i ->
						taskRespository.createIfNotExists(new Execution(now.minusSeconds(new Random().nextInt(10000)), oneTimeTask.instance("id" + i)))
		);
		List<Execution> due = taskRespository.getDue(now);
		assertThat(due, hasSize(100));

		List<Execution> sortedDue = new ArrayList<>(due);
		Collections.sort(sortedDue, Comparator.comparing(Execution::getExeecutionTime));
		assertThat(due, is(sortedDue));
	}

	@Test
	public void picked_executions_should_not_be_returned_as_due() {
		LocalDateTime now = LocalDateTime.now();
		taskRespository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
		List<Execution> due = taskRespository.getDue(now);
		assertThat(due, hasSize(1));

		taskRespository.pick(due.get(0));
		assertThat(taskRespository.getDue(now), hasSize(0));
	}

	@Test
	public void reschedule_should_move_execution_in_time() {
		LocalDateTime now = LocalDateTime.now();
		taskRespository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
		List<Execution> due = taskRespository.getDue(now);
		assertThat(due, hasSize(1));

		Execution execution = due.get(0);
		taskRespository.pick(execution);
		taskRespository.reschedule(execution, now.plusMinutes(1));

		assertThat(taskRespository.getDue(now), hasSize(0));
		assertThat(taskRespository.getDue(now.plusMinutes(1)), hasSize(1));
	}
}
