package com.kagkarlsson.scheduler;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.*;

public class JdbcTaskRepositoryTest {

	@Rule
	public HsqlTestDatabaseRule DB = new HsqlTestDatabaseRule();
	private JdbcTaskRepository taskRepository;
	private OneTimeTask oneTimeTask;
	private RecurringTask recurringTask;

	@Before
	public void setUp() {
		oneTimeTask = new OneTimeTask("OneTime", instance -> {
		});
		recurringTask = new RecurringTask("RecurringTask", Duration.ofSeconds(1), instance -> {
		});
		List<Task> knownTasks = new ArrayList<>();
		knownTasks.add(oneTimeTask);
		knownTasks.add(recurringTask);
		taskRepository = new JdbcTaskRepository(DB.getDataSource(), new TaskResolver(knownTasks, TaskResolver.OnCannotResolve.WARN_ON_UNRESOLVED));
	}

	@Test
	public void test_createIfNotExists() {
		LocalDateTime now = LocalDateTime.now();

		TaskInstance instance1 = oneTimeTask.instance("id1");
		TaskInstance instance2 = oneTimeTask.instance("id2");

		assertTrue(taskRepository.createIfNotExists(new Execution(now, instance1)));
		assertFalse(taskRepository.createIfNotExists(new Execution(now, instance1)));

		assertTrue(taskRepository.createIfNotExists(new Execution(now, instance2)));
	}

	@Test
	public void get_due_should_only_include_due_executions() {
		LocalDateTime now = LocalDateTime.now();

		taskRepository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
		assertThat(taskRepository.getDue(now), hasSize(1));
		assertThat(taskRepository.getDue(now.minusSeconds(1)), hasSize(0));
	}

}
