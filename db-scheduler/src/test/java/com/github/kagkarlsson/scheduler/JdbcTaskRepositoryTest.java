package com.github.kagkarlsson.scheduler;

import static com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter.all;
import static com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter.onlyResolved;
import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static java.time.Duration.ofDays;
import static java.time.Duration.ofMinutes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.unruly.matchers.OptionalMatchers;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.exceptions.FailedToScheduleBatchException;
import com.github.kagkarlsson.scheduler.helper.TestableListener;
import com.github.kagkarlsson.scheduler.helper.TimeHelper;
import com.github.kagkarlsson.scheduler.jdbc.DeactivationUpdate;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class JdbcTaskRepositoryTest {

  public static final String SCHEDULER_NAME = "scheduler1";
  private static final int POLLING_LIMIT = 10_000;
  @RegisterExtension public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

  private JdbcTaskRepository taskRepository;
  private OneTimeTask<Void> oneTimeTask;
  private OneTimeTask<Void> alternativeOneTimeTask;
  private OneTimeTask<Integer> oneTimeTaskWithData;
  private TaskResolver taskResolver;
  private TestableListener testableListener;
  private Instant truncatedNow;

  @BeforeEach
  public void setUp() {
    oneTimeTask = TestTasks.oneTime("OneTime", Void.class, TestTasks.DO_NOTHING);
    alternativeOneTimeTask =
        TestTasks.oneTime("AlternativeOneTime", Void.class, TestTasks.DO_NOTHING);
    oneTimeTaskWithData =
        TestTasks.oneTime("OneTimeWithData", Integer.class, new TestTasks.DoNothingHandler<>());
    List<Task<?>> knownTasks = new ArrayList<>();
    knownTasks.add(oneTimeTask);
    knownTasks.add(oneTimeTaskWithData);
    knownTasks.add(alternativeOneTimeTask);
    testableListener = new TestableListener(true, Collections.emptyList());
    taskResolver =
        new TaskResolver(new SchedulerListeners(testableListener), new SystemClock(), knownTasks);
    taskRepository = createRepository(taskResolver, false);

    truncatedNow = TimeHelper.truncatedInstantNow();
  }

  @Test
  public void test_createIfNotExists() {
    Instant now = TimeHelper.truncatedInstantNow();

    TaskInstance<Void> instance1 = oneTimeTask.instance("id1");
    TaskInstance<Void> instance2 = oneTimeTask.instance("id2");

    assertTrue(taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance1, now)));
    assertFalse(taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance1, now)));

    assertTrue(taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance2, now)));
  }

  @Test
  public void test_createBatch() {
    Instant now = TimeHelper.truncatedInstantNow();
    TaskInstance<Void> instance1 = oneTimeTask.instance("id1");
    TaskInstance<Void> instance2 = oneTimeTask.instance("id2");
    List<ScheduledTaskInstance> executions =
        List.of(
            new ScheduledTaskInstance(instance1, now), new ScheduledTaskInstance(instance2, now));

    taskRepository.createBatch(executions);

    assertTrue(taskRepository.getExecution(instance1).isPresent());
    assertTrue(taskRepository.getExecution(instance2).isPresent());
  }

  @Test
  public void test_createBatch_fails_if_any_execution_already_exists() {
    Instant now = TimeHelper.truncatedInstantNow();
    TaskInstance<Void> instance1 = oneTimeTask.instance("id1");
    TaskInstance<Void> instance2 = oneTimeTask.instance("id2");
    List<ScheduledTaskInstance> executions =
        List.of(
            new ScheduledTaskInstance(instance1, now), new ScheduledTaskInstance(instance2, now));
    taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance2, now));

    assertThrows(
        FailedToScheduleBatchException.class, () -> taskRepository.createBatch(executions));
  }

  @Test
  public void test_replace() {
    Instant now = TimeHelper.truncatedInstantNow();

    TaskInstance<Integer> instance1 = oneTimeTaskWithData.instance("id1", 1);
    SchedulableInstance<Integer> instance2 = oneTimeTaskWithData.schedulableInstance("id2", 2);

    assertTrue(taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance1, now)));
    Execution scheduled = taskRepository.getExecution(instance1).get();
    assertEquals(1, scheduled.taskInstance.getData());

    taskRepository.replace(scheduled, instance2);
    Execution replaced = taskRepository.getExecution(instance2.getTaskInstance()).get();
    assertEquals(2, replaced.taskInstance.getData());
    assertEquals("id2", replaced.taskInstance.getId());
  }

  @Test
  public void get_due_should_only_include_due_executions() {
    Instant now = TimeHelper.truncatedInstantNow();

    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(oneTimeTask.instance("id1"), now));
    assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(1));
    assertThat(taskRepository.getDue(now.minusSeconds(1), POLLING_LIMIT), hasSize(0));
  }

  @Test
  public void get_due_should_honor_max_results_limit() {
    Instant now = TimeHelper.truncatedInstantNow();

    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(oneTimeTask.instance("id1"), now));
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(oneTimeTask.instance("id2"), now));
    assertThat(taskRepository.getDue(now, 1), hasSize(1));
    assertThat(taskRepository.getDue(now, 2), hasSize(2));
  }

  @Test
  public void get_due_should_be_sorted() {
    Instant now = TimeHelper.truncatedInstantNow();
    IntStream.range(0, 100)
        .forEach(
            i ->
                taskRepository.createIfNotExists(
                    new SchedulableTaskInstance<>(
                        oneTimeTask.instance("id" + i),
                        now.minusSeconds(new Random().nextInt(10000)))));
    List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
    assertThat(due, hasSize(100));

    List<Execution> sortedDue = new ArrayList<>(due);
    sortedDue.sort(Comparator.comparing(Execution::getExecutionTime));
    assertThat(due, is(sortedDue));
  }

  @Test
  public void get_due_should_be_sorted_by_priority_when_enabled() {
    JdbcTaskRepository taskRespositoryWithPriority = createRepository(taskResolver, true);
    Instant now = TimeHelper.truncatedInstantNow();
    SchedulableTaskInstance<Void> id1 =
        new SchedulableTaskInstance<>(
            oneTimeTask.instanceBuilder("id1").priority(1).build(), now.minus(ofDays(1)));
    SchedulableTaskInstance<Void> id2 =
        new SchedulableTaskInstance<>(
            oneTimeTask.instanceBuilder("id2").priority(10).build(), now.minus(ofDays(2)));
    SchedulableTaskInstance<Void> id3 =
        new SchedulableTaskInstance<>(
            oneTimeTask.instanceBuilder("id3").priority(5).build(), now.minus(ofDays(3)));

    Stream.of(id1, id2, id3).forEach(taskRespositoryWithPriority::createIfNotExists);

    List<String> orderedByPriority =
        taskRespositoryWithPriority.getDue(now, POLLING_LIMIT).stream()
            .map(Execution::getId)
            .collect(Collectors.toList());
    assertThat(orderedByPriority, contains("id2", "id3", "id1"));

    // default task-repository should have no order
    List<String> orderedByExecutionTime =
        taskRepository.getDue(now, POLLING_LIMIT).stream()
            .map(Execution::getId)
            .collect(Collectors.toList());
    assertThat(orderedByExecutionTime, contains("id3", "id2", "id1"));
  }

  @Test
  public void get_due_should_not_include_previously_unresolved() {
    Instant now = TimeHelper.truncatedInstantNow();
    final OneTimeTask<Void> unresolved1 =
        TestTasks.oneTime("unresolved1", Void.class, TestTasks.DO_NOTHING);
    final OneTimeTask<Void> unresolved2 =
        TestTasks.oneTime("unresolved2", Void.class, TestTasks.DO_NOTHING);
    final OneTimeTask<Void> unresolved3 =
        TestTasks.oneTime("unresolved3", Void.class, TestTasks.DO_NOTHING);

    assertThat(taskResolver.getUnresolved(), hasSize(0));

    // 1
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(unresolved1.instance("id"), now));
    assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskResolver.getUnresolved(), hasSize(1));
    assertEquals(1, testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK));

    assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskResolver.getUnresolved(), hasSize(1));
    assertEquals(
        1,
        testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK),
        "Execution should not have have been in the ResultSet");

    // 1, 2
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(unresolved2.instance("id"), now));
    assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskResolver.getUnresolved(), hasSize(2));
    assertEquals(2, testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK));

    // 1, 2, 3
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(unresolved3.instance("id"), now));
    assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskResolver.getUnresolved(), hasSize(3));
    assertEquals(3, testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK));
  }

  @Test
  public void picked_executions_should_not_be_returned_as_due() {
    Instant now = TimeHelper.truncatedInstantNow();
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(oneTimeTask.instance("id1"), now));
    List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
    assertThat(due, hasSize(1));

    taskRepository.pick(due.get(0), now);
    assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
  }

  @Test
  public void picked_execution_should_have_information_about_which_scheduler_processes_it() {
    Instant now = TimeHelper.truncatedInstantNow();
    final TaskInstance<Void> instance = oneTimeTask.instance("id1");
    taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance, now));
    List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
    assertThat(due, hasSize(1));
    taskRepository.pick(due.get(0), now);

    final Optional<Execution> pickedExecution = taskRepository.getExecution(instance);
    assertThat(pickedExecution.isPresent(), is(true));
    assertThat(pickedExecution.get().picked, is(true));
    assertThat(pickedExecution.get().pickedBy, is(SCHEDULER_NAME));
    assertThat(pickedExecution.get().lastHeartbeat, notNullValue());
    assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
  }

  @Test
  public void should_not_be_able_to_pick_execution_that_has_been_rescheduled() {
    Instant now = TimeHelper.truncatedInstantNow();
    final TaskInstance<Void> instance = oneTimeTask.instance("id1");
    taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance, now));

    List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
    assertThat(due, hasSize(1));
    final Execution execution = due.get(0);
    final Optional<Execution> pickedExecution = taskRepository.pick(execution, now);
    assertThat(pickedExecution.isPresent(), is(true));
    taskRepository.reschedule(pickedExecution.get(), now.plusSeconds(1), now, null, 0);

    assertThat(taskRepository.pick(pickedExecution.get(), now).isPresent(), is(false));
  }

  @Test
  public void reschedule_should_move_execution_in_time() {
    Instant now = TimeHelper.truncatedInstantNow();
    final TaskInstance<Void> instance = oneTimeTask.instance("id1");
    taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance, now));
    List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
    assertThat(due, hasSize(1));

    Execution execution = due.get(0);
    final Optional<Execution> pickedExecution = taskRepository.pick(execution, now);
    final Instant nextExecutionTime = now.plus(ofMinutes(1));
    taskRepository.reschedule(pickedExecution.get(), nextExecutionTime, now, null, 0);

    assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskRepository.getDue(nextExecutionTime, POLLING_LIMIT), hasSize(1));

    final Optional<Execution> nextExecution = taskRepository.getExecution(instance);
    assertTrue(nextExecution.isPresent());
    assertThat(nextExecution.get().picked, is(false));
    assertThat(nextExecution.get().pickedBy, nullValue());
    assertThat(nextExecution.get().executionTime, is(nextExecutionTime));
  }

  @Test
  public void reschedule_should_persist_consecutive_failures() {
    Instant now = TimeHelper.truncatedInstantNow();
    final TaskInstance<Void> instance = oneTimeTask.instance("id1");
    taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance, now));
    List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
    assertThat(due, hasSize(1));

    Execution execution = due.get(0);
    final Optional<Execution> pickedExecution = taskRepository.pick(execution, now);
    final Instant nextExecutionTime = now.plus(ofMinutes(1));
    taskRepository.reschedule(
        pickedExecution.get(), nextExecutionTime, now.minusSeconds(1), now, 1);

    final Optional<Execution> nextExecution = taskRepository.getExecution(instance);
    assertTrue(nextExecution.isPresent());
    assertThat(nextExecution.get().consecutiveFailures, is(1));
  }

  @Test
  public void reschedule_should_update_data_if_specified() {
    Instant now = TimeHelper.truncatedInstantNow();
    final TaskInstance<Integer> instance = oneTimeTaskWithData.instance("id1", 1);
    taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance, now));

    Execution created = taskRepository.getExecution(instance).get();
    assertEquals(created.taskInstance.getData(), 1);

    final Instant nextExecutionTime = now.plus(ofMinutes(1));
    taskRepository.reschedule(created, nextExecutionTime, 2, now, null, 0);

    final Execution rescheduled = taskRepository.getExecution(instance).get();
    assertEquals(rescheduled.taskInstance.getData(), 2);
  }

  @Test
  public void test_get_failing_executions() {
    Instant now = TimeHelper.truncatedInstantNow();
    final TaskInstance<Void> instance = oneTimeTask.instance("id1");
    taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance, now));

    List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
    assertThat(due, hasSize(1));

    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

    taskRepository.reschedule(getSingleDueExecution(), now, now, null, 0);
    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

    taskRepository.reschedule(getSingleDueExecution(), now, null, now, 1);
    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
    assertThat(taskRepository.getExecutionsFailingLongerThan(ofMinutes(1)), hasSize(1));
    assertThat(taskRepository.getExecutionsFailingLongerThan(ofDays(1)), hasSize(1));

    taskRepository.reschedule(getSingleDueExecution(), now, now.minus(ofMinutes(1)), now, 1);
    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofSeconds(1)), hasSize(1));
    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofHours(1)), hasSize(0));
  }

  @Test
  public void
      get_failing_executions_should_not_return_previously_failed_but_currently_successful() {
    Instant third = TimeHelper.truncatedInstantNow();
    Instant second = third.minus(ofMinutes(1));
    Instant first = second.minus(ofMinutes(1));

    Instant timeToRun = first;

    final TaskInstance<Void> instance = oneTimeTask.instance("id1");
    taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance, timeToRun));

    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

    // simulate success
    taskRepository.reschedule(getSingleDueExecution(), timeToRun, first, null, 0);
    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

    // simulate failure
    taskRepository.reschedule(getSingleDueExecution(), timeToRun, first, second, 1);
    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
    assertThat(taskRepository.getExecutionsFailingLongerThan(ofMinutes(5)), hasSize(0));

    // simulate success
    taskRepository.reschedule(getSingleDueExecution(), timeToRun, third, second, 0);
    assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));
  }

  @Test
  public void get_scheduled_executions() {
    Instant now = TimeHelper.truncatedInstantNow();
    IntStream.range(0, 100)
        .forEach(
            i ->
                taskRepository.createIfNotExists(
                    new SchedulableTaskInstance<>(
                        oneTimeTask.instance("id" + i),
                        now.plus(new Random().nextInt(10), ChronoUnit.HOURS))));
    final List<Execution> beforePick = getScheduledExecutions(all().withPicked(false));
    assertThat(beforePick, hasSize(100));

    taskRepository.pick(beforePick.get(0), Instant.now());

    assertThat(getScheduledExecutions(all().withPicked(false)), hasSize(99));
    assertThat(getScheduledExecutions(all()), hasSize(100));
  }

  private List<Execution> getScheduledExecutions(ScheduledExecutionsFilter filter) {
    List<Execution> beforePick = new ArrayList<>();
    taskRepository.getScheduledExecutions(filter, beforePick::add);
    return beforePick;
  }

  @Test
  public void get_scheduled_by_task_name() {
    Instant now = TimeHelper.truncatedInstantNow();
    final SchedulableTaskInstance<Void> execution1 =
        new SchedulableTaskInstance<>(
            oneTimeTask.instance("id" + 1), now.plus(new Random().nextInt(10), ChronoUnit.HOURS));
    taskRepository.createIfNotExists(execution1);
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(
            oneTimeTask.instance("id" + 2), now.plus(new Random().nextInt(10), ChronoUnit.HOURS)));
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(
            alternativeOneTimeTask.instance("id" + 3),
            now.plus(new Random().nextInt(10), ChronoUnit.HOURS)));

    taskRepository.pick(
        taskRepository.getExecution(execution1.getTaskInstance()).get(), Instant.now());
    assertThat(getScheduledExecutions(all().withPicked(true), oneTimeTask.getName()), hasSize(1));
    assertThat(getScheduledExecutions(all().withPicked(false), oneTimeTask.getName()), hasSize(1));
    assertThat(getScheduledExecutions(all(), oneTimeTask.getName()), hasSize(2));

    assertThat(
        getScheduledExecutions(all().withPicked(false), alternativeOneTimeTask.getName()),
        hasSize(1));
    assertThat(getScheduledExecutions(all().withPicked(false), "non-existing"), empty());
  }

  @Test
  public void get_dead_executions_should_not_include_previously_unresolved() {
    Instant now = TimeHelper.truncatedInstantNow();

    // 1
    final Instant timeDied = now.minus(ofDays(5));
    createDeadExecution(oneTimeTask.instance("id1"), timeDied);

    TaskResolver taskResolverMissingTask =
        new TaskResolver(new SchedulerListeners(testableListener), new SystemClock(), List.of());
    JdbcTaskRepository repoMissingTask = createRepository(taskResolverMissingTask, false);

    assertThat(taskResolverMissingTask.getUnresolved(), hasSize(0));
    assertEquals(0, testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK));

    assertThat(repoMissingTask.getDeadExecutions(timeDied), hasSize(0));
    assertThat(taskResolverMissingTask.getUnresolved(), hasSize(1));
    assertEquals(1, testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK));

    assertThat(repoMissingTask.getDeadExecutions(timeDied), hasSize(0));
    assertThat(taskResolverMissingTask.getUnresolved(), hasSize(1));
    assertEquals(1, testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK));
  }

  @Test
  public void get_scheduled_executions_should_work_with_unresolved() {
    Instant now = TimeHelper.truncatedInstantNow();
    String taskName = "unresolved1";
    final OneTimeTask<Void> unresolved1 =
        TestTasks.oneTime(taskName, Void.class, TestTasks.DO_NOTHING);
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(unresolved1.instance("id"), now));
    assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskResolver.getUnresolved(), hasSize(1));

    assertThat(getScheduledExecutions(ScheduledExecutionsFilter.onlyResolved()), hasSize(0));
    assertThat(
        getScheduledExecutions(ScheduledExecutionsFilter.onlyResolved(), taskName), hasSize(0));
    assertThat(getScheduledExecutions(all()), hasSize(1));
    assertThat(getScheduledExecutions(all(), taskName), hasSize(1));
  }

  @Test
  public void lockAndGetDue_should_pick_due() {
    Instant now = Instant.now();
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(oneTimeTask.instance("future1"), now.plusSeconds(10)));
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(oneTimeTask.instance("id1"), now));
    List<Execution> picked = taskRepository.lockAndGetDue(now, POLLING_LIMIT);
    assertThat(picked, hasSize(1));

    // should not be able to pick the same execution twice
    assertThat(taskRepository.lockAndGetDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskRepository.pick(picked.get(0), now), OptionalMatchers.empty());
  }

  @Test
  public void lockAndGetDue_should_not_include_previously_unresolved() {
    Instant now = TimeHelper.truncatedInstantNow();
    final OneTimeTask<Void> unresolved1 =
        TestTasks.oneTime("unresolved1", Void.class, TestTasks.DO_NOTHING);

    assertThat(taskResolver.getUnresolved(), hasSize(0));

    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(unresolved1.instance("id"), now));
    assertThat(taskRepository.lockAndGetDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskRepository.lockAndGetDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskResolver.getUnresolved(), hasSize(1));
    assertEquals(1, testableListener.getCount(SchedulerEventType.UNRESOLVED_TASK));
  }

  @Test
  public void lockAndFetchGeneric_happy() {
    Instant now = Instant.now();
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(oneTimeTask.instance("future1"), now.plusSeconds(10)));
    taskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(oneTimeTask.instance("id1"), now));
    List<Execution> picked = taskRepository.lockAndFetchGeneric(now, POLLING_LIMIT);
    assertThat(picked, hasSize(1));

    // should not be able to pick the same execution twice
    assertThat(taskRepository.lockAndFetchGeneric(now, POLLING_LIMIT), hasSize(0));
    assertThat(taskRepository.pick(picked.get(0), now), OptionalMatchers.empty());
  }

  @Test
  public void deactivated_executions_should_not_be_due() {
    createExecution(oneTimeTask.instance("active"), truncatedNow);
    var toDeactivate = createExecution(oneTimeTask.instance("toDeactivate"), truncatedNow);

    taskRepository.deactivate(toDeactivate, DeactivationUpdate.toState(State.PAUSED).build());

    assertThat(toIds(taskRepository.getDue(truncatedNow, POLLING_LIMIT))).containsExactly("active");

    assertThat(taskRepository.getDeactivatedExecutions())
        .satisfiesExactly(
            e -> {
              assertThat(e.taskInstance.getId()).isEqualTo("toDeactivate");
              assertThat(e.state).isEqualTo(State.PAUSED);
            });
  }

  private <T> Execution createExecution(TaskInstance<T> instance, Instant executionTime) {
    taskRepository.createIfNotExists(SchedulableInstance.of(instance, executionTime));
    return taskRepository
        .getExecution(instance)
        .orElseThrow(() -> new IllegalStateException("Execution should exist"));
  }

  private List<String> toIds(List<Execution> due) {
    return due.stream().map(Execution::getId).toList();
  }

  private List<Execution> getScheduledExecutions(
      ScheduledExecutionsFilter filter, String taskName) {
    List<Execution> alternativeTasks = new ArrayList<>();
    taskRepository.getScheduledExecutions(filter, taskName, alternativeTasks::add);
    return alternativeTasks;
  }

  private JdbcTaskRepository createRepository(TaskResolver taskResolver, boolean orderByPriority) {
    return new JdbcTaskRepository(
        DB.getDataSource(),
        false,
        DEFAULT_TABLE_NAME,
        taskResolver,
        new SchedulerName.Fixed(SCHEDULER_NAME),
        orderByPriority,
        new SystemClock());
  }

  private void createDeadExecution(TaskInstance<Void> taskInstance, Instant timeDied) {
    taskRepository.createIfNotExists(new SchedulableTaskInstance<>(taskInstance, timeDied));
    final Execution due = getSingleExecution();

    final Optional<Execution> picked = taskRepository.pick(due, timeDied);
    taskRepository.updateHeartbeat(picked.get(), timeDied);
  }

  private Execution getSingleDueExecution() {
    List<Execution> due = taskRepository.getDue(Instant.now(), POLLING_LIMIT);
    return due.get(0);
  }

  private Execution getSingleExecution() {
    List<Execution> executions = new ArrayList<>();
    taskRepository.getScheduledExecutions(onlyResolved().withPicked(false), executions::add);
    return executions.get(0);
  }

  @Test
  public void removeOldDeactivatedExecutions_deletes_correct_states_and_respects_time_and_limit() {
    Instant oldTime = truncatedNow.minus(ofDays(30));
    Instant recentTime = truncatedNow.minus(ofDays(7));
    Instant ageLimit = truncatedNow.minus(ofDays(14));
    Set<State> statesToDelete =
        EnumSet.of(State.COMPLETE, State.PAUSED, State.FAILED, State.WAITING);

    // Should be deleted
    createDeactivatedExecution("oldComplete", oldTime, State.COMPLETE);
    createDeactivatedExecution("oldPaused", oldTime, State.PAUSED);
    createDeactivatedExecution("oldFailed", oldTime, State.FAILED);
    createDeactivatedExecution("oldWaiting", oldTime, State.WAITING);

    // Should NOT be deleted
    createExecution(oneTimeTask.instance("activeNull"), oldTime);
    createDeactivatedExecution("oldRecord", oldTime, State.RECORD);
    createDeactivatedExecution("recentComplete", recentTime, State.COMPLETE);

    // Execute deletion
    int removed = taskRepository.removeOldDeactivatedExecutions(statesToDelete, ageLimit, 1000);

    assertEquals(4, removed);
    assertDeleted("oldComplete");
    assertDeleted("oldPaused");
    assertDeleted("oldFailed");
    assertDeleted("oldWaiting");

    assertNotDeleted("activeNull");
    assertNotDeleted("oldRecord");
    assertNotDeleted("recentComplete");
  }

  @Test
  public void removeOldDeactivatedExecutions_respects_limit() {
    Instant oldTime = truncatedNow.minus(ofDays(30));
    for (int i = 0; i < 5; i++) {
      createDeactivatedExecution("limited" + i, oldTime, State.COMPLETE);
    }

    int removed =
        taskRepository.removeOldDeactivatedExecutions(
            EnumSet.of(State.COMPLETE), truncatedNow.minus(ofDays(14)), 2);

    assertEquals(2, removed);
    assertEquals(3, taskRepository.getDeactivatedExecutions().size());
  }

  private void createDeactivatedExecution(String id, Instant executionTime, State state) {
    var execution = createExecution(oneTimeTask.instance(id), executionTime);
    taskRepository.deactivate(execution, DeactivationUpdate.toState(state).build());
  }

  private void assertDeleted(String id) {
    assertThat(taskRepository.getExecution(oneTimeTask.instance(id)))
        .as("Execution '%s' should be deleted", id)
        .isEmpty();
  }

  private void assertNotDeleted(String id) {
    assertThat(taskRepository.getExecution(oneTimeTask.instance(id)))
        .as("Execution '%s' should NOT be deleted", id)
        .isPresent();
  }
}
