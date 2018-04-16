package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.*;

public class JdbcTaskRepositoryTest {

	public static final String SCHEDULER_NAME = "scheduler1";
	@Rule
	public EmbeddedPostgresqlRule DB = new EmbeddedPostgresqlRule(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);

	private JdbcTaskRepository taskRepository;
	private OneTimeTask<Void> oneTimeTask;
	private OneTimeTask<Void> alternativeOneTimeTask;
	private OneTimeTask<Integer> oneTimeTaskWithData;

	@Before
	public void setUp() {
		oneTimeTask = TestTasks.oneTime("OneTime", Void.class, TestTasks.DO_NOTHING);
		alternativeOneTimeTask = TestTasks.oneTime("AlternativeOneTime", Void.class, TestTasks.DO_NOTHING);
		oneTimeTaskWithData = TestTasks.oneTime("OneTimeWithData", Integer.class, new TestTasks.DoNothingHandler<>());
		List<Task<?>> knownTasks = new ArrayList<>();
		knownTasks.add(oneTimeTask);
		knownTasks.add(oneTimeTaskWithData);
		knownTasks.add(alternativeOneTimeTask);
		taskRepository = new JdbcTaskRepository(DB.getDataSource(), new TaskResolver(knownTasks), new SchedulerName.Fixed(SCHEDULER_NAME));
	}

	@Test
	public void test_createIfNotExists() {
		Instant now = Instant.now();

		TaskInstance<Void> instance1 = oneTimeTask.instance("id1");
		TaskInstance<Void> instance2 = oneTimeTask.instance("id2");

		assertTrue(taskRepository.createIfNotExists(new Execution(now, instance1)));
		assertFalse(taskRepository.createIfNotExists(new Execution(now, instance1)));

		assertTrue(taskRepository.createIfNotExists(new Execution(now, instance2)));
	}

	@Test
	public void get_due_should_only_include_due_executions() {
		Instant now = Instant.now();

		taskRepository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
		assertThat(taskRepository.getDue(now), hasSize(1));
		assertThat(taskRepository.getDue(now.minusSeconds(1)), hasSize(0));
	}

	@Test
	public void get_due_should_honor_max_results_limit() {
		Instant now = Instant.now();

		taskRepository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
		taskRepository.createIfNotExists(new Execution(now, oneTimeTask.instance("id2")));
		assertThat(taskRepository.getDue(now, 1), hasSize(1));
		assertThat(taskRepository.getDue(now, 2), hasSize(2));
	}

	@Test
	public void get_due_should_be_sorted() {
		Instant now = Instant.now();
		IntStream.range(0, 100).forEach(i ->
						taskRepository.createIfNotExists(new Execution(now.minusSeconds(new Random().nextInt(10000)), oneTimeTask.instance("id" + i)))
		);
		List<Execution> due = taskRepository.getDue(now);
		assertThat(due, hasSize(100));

		List<Execution> sortedDue = new ArrayList<>(due);
		sortedDue.sort(Comparator.comparing(Execution::getExecutionTime));
		assertThat(due, is(sortedDue));
	}

	@Test
	public void picked_executions_should_not_be_returned_as_due() {
		Instant now = Instant.now();
		taskRepository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
		List<Execution> due = taskRepository.getDue(now);
		assertThat(due, hasSize(1));

		taskRepository.pick(due.get(0), now);
		assertThat(taskRepository.getDue(now), hasSize(0));
	}

	@Test
	public void picked_execution_should_have_information_about_which_scheduler_processes_it() {
		Instant now = Instant.now();
		final TaskInstance<Void> instance = oneTimeTask.instance("id1");
		taskRepository.createIfNotExists(new Execution(now, instance));
		List<Execution> due = taskRepository.getDue(now);
		assertThat(due, hasSize(1));
		taskRepository.pick(due.get(0), now);

		final Optional<Execution> pickedExecution = taskRepository.getExecution(instance);
		assertThat(pickedExecution.isPresent(), is(true));
		assertThat(pickedExecution.get().picked, is(true));
		assertThat(pickedExecution.get().pickedBy, is(SCHEDULER_NAME));
		assertThat(pickedExecution.get().lastHeartbeat, notNullValue());
		assertThat(taskRepository.getDue(now), hasSize(0));
	}

	@Test
	public void should_not_be_able_to_pick_execution_that_has_been_rescheduled() {
		Instant now = Instant.now();
		final TaskInstance<Void> instance = oneTimeTask.instance("id1");
		taskRepository.createIfNotExists(new Execution(now, instance));

		List<Execution> due = taskRepository.getDue(now);
		assertThat(due, hasSize(1));
		final Execution execution = due.get(0);
		final Optional<Execution> pickedExecution = taskRepository.pick(execution, now);
		assertThat(pickedExecution.isPresent(), is(true));
		taskRepository.reschedule(pickedExecution.get(), now.plusSeconds(1), now, null);

		assertThat(taskRepository.pick(pickedExecution.get(), now).isPresent(), is(false));
	}

	@Test
	public void reschedule_should_move_execution_in_time() {
		Instant now = Instant.now();
		final TaskInstance<Void> instance = oneTimeTask.instance("id1");
		taskRepository.createIfNotExists(new Execution(now, instance));
		List<Execution> due = taskRepository.getDue(now);
		assertThat(due, hasSize(1));

		Execution execution = due.get(0);
		final Optional<Execution> pickedExecution = taskRepository.pick(execution, now);
		final Instant nextExecutionTime = now.plus(Duration.ofMinutes(1));
		taskRepository.reschedule(pickedExecution.get(), nextExecutionTime, now, null);

		assertThat(taskRepository.getDue(now), hasSize(0));
		assertThat(taskRepository.getDue(nextExecutionTime), hasSize(1));

		final Optional<Execution> nextExecution = taskRepository.getExecution(instance);
		assertTrue(nextExecution.isPresent());
		assertThat(nextExecution.get().picked, is(false));
		assertThat(nextExecution.get().pickedBy, nullValue());
		assertThat(nextExecution.get().executionTime, is(nextExecutionTime));
	}

	@Test
	public void reschedule_should_update_data_if_specified() {
		Instant now = Instant.now();
		final TaskInstance<Integer> instance = oneTimeTaskWithData.instance("id1", 1);
		taskRepository.createIfNotExists(new Execution(now, instance));

		Execution created = taskRepository.getExecution(instance).get();
		assertEquals(created.taskInstance.getData(), 1);

		final Instant nextExecutionTime = now.plus(Duration.ofMinutes(1));
		taskRepository.reschedule(created, nextExecutionTime, 2, now, null);

		final Execution rescheduled = taskRepository.getExecution(instance).get();
		assertEquals(rescheduled.taskInstance.getData(), 2);
	}

	@Test
	public void test_get_failing_executions() {
		Instant now = Instant.now();
		final TaskInstance<Void> instance = oneTimeTask.instance("id1");
		taskRepository.createIfNotExists(new Execution(now, instance));

		List<Execution> due = taskRepository.getDue(now);
		assertThat(due, hasSize(1));

		assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

		taskRepository.reschedule(getSingleExecution(), now, now, null);
		assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

		taskRepository.reschedule(getSingleExecution(), now, null, now);
		assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
		assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofMinutes(1)), hasSize(1));
		assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofDays(1)), hasSize(1));

		taskRepository.reschedule(getSingleExecution(), now, now.minus(Duration.ofMinutes(1)), now);
		assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
		assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofSeconds(1)), hasSize(1));
		assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofHours(1)), hasSize(0));
	}

	@Test
	public void get_scheduled_executions() {
		Instant now = Instant.now();
		IntStream.range(0, 100).forEach(i ->
				taskRepository.createIfNotExists(new Execution(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.instance("id" + i)))
		);
		List<Execution> beforePick = taskRepository.getScheduledExecutions();
		assertThat(beforePick, hasSize(100));

		taskRepository.pick(beforePick.get(0), Instant.now());
		List<Execution> afterPick = taskRepository.getScheduledExecutions();
		assertThat(afterPick, hasSize(99));
	}

	@Test
	public void get_scheduled_with_50_as_limit() {
		Instant now = Instant.now();
		IntStream.range(0, 100).forEach(i ->
				taskRepository.createIfNotExists(new Execution(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.instance("id" + i)))
		);
		List<Execution> beforePick = taskRepository.getScheduled(50);
		assertThat(beforePick, hasSize(50));
	}

	@Test
	public void get_scheduled_by_task_name() {
		Instant now = Instant.now();
		taskRepository.createIfNotExists(new Execution(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.instance("id" + 1)));
		taskRepository.createIfNotExists(new Execution(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.instance("id" + 2)));
		taskRepository.createIfNotExists(new Execution(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), alternativeOneTimeTask.instance("id" + 3)));

		List<Execution> scheduledByTaskName = taskRepository.getScheduledExecutions(oneTimeTask.getName());
		assertThat(scheduledByTaskName, hasSize(2));

		List<Execution> alternativeTasks = taskRepository.getScheduledExecutions(alternativeOneTimeTask.getName());
		assertThat(alternativeTasks, hasSize(1));

		List<Execution> empty = taskRepository.getScheduledExecutions("non-existing");
		assertThat(empty, empty());
	}

	private Execution getSingleExecution() {
		List<Execution> due = taskRepository.getDue(Instant.now());
		return due.get(0);
	}

}
