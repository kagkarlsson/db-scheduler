package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.helper.TimeHelper;
import com.github.kagkarlsson.scheduler.jdbc.DefaultJdbcCustomization;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.IntStream;

import static com.github.kagkarlsson.scheduler.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;


@SuppressWarnings("unchecked")
public class JdbcTaskRepositoryTest {

    public static final String SCHEDULER_NAME = "scheduler1";
    private static final int POLLING_LIMIT = 10_000;
    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

    private JdbcTaskRepository taskRepository;
    private OneTimeTask<Void> oneTimeTask;
    private OneTimeTask<Void> alternativeOneTimeTask;
    private OneTimeTask<Integer> oneTimeTaskWithData;
    private TaskResolver taskResolver;
    private TestableRegistry testableRegistry;

    @BeforeEach
    public void setUp() {
        oneTimeTask = TestTasks.oneTime("OneTime", Void.class, TestTasks.DO_NOTHING);
        alternativeOneTimeTask = TestTasks.oneTime("AlternativeOneTime", Void.class, TestTasks.DO_NOTHING);
        oneTimeTaskWithData = TestTasks.oneTime("OneTimeWithData", Integer.class, new TestTasks.DoNothingHandler<>());
        List<Task<?>> knownTasks = new ArrayList<>();
        knownTasks.add(oneTimeTask);
        knownTasks.add(oneTimeTaskWithData);
        knownTasks.add(alternativeOneTimeTask);
        testableRegistry = new TestableRegistry(true, Collections.emptyList());
        taskResolver = new TaskResolver(testableRegistry, knownTasks);
        taskRepository = new JdbcTaskRepository(DB.getDataSource(), DEFAULT_TABLE_NAME, taskResolver, new SchedulerName.Fixed(SCHEDULER_NAME));
    }

    @Test
    public void test_createIfNotExists() {
        Instant now = TimeHelper.truncatedInstantNow();

        TaskInstance<Void> instance1 = oneTimeTask.instance("id1");
        TaskInstance<Void> instance2 = oneTimeTask.instance("id2");

        assertTrue(taskRepository.createIfNotExists(new Execution(now, instance1)));
        assertFalse(taskRepository.createIfNotExists(new Execution(now, instance1)));

        assertTrue(taskRepository.createIfNotExists(new Execution(now, instance2)));
    }

    @Test
    public void get_due_should_only_include_due_executions() {
        Instant now = TimeHelper.truncatedInstantNow();

        taskRepository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(1));
        assertThat(taskRepository.getDue(now.minusSeconds(1), POLLING_LIMIT), hasSize(0));
    }

    @Test
    public void get_due_should_honor_max_results_limit() {
        Instant now = TimeHelper.truncatedInstantNow();

        taskRepository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
        taskRepository.createIfNotExists(new Execution(now, oneTimeTask.instance("id2")));
        assertThat(taskRepository.getDue(now, 1), hasSize(1));
        assertThat(taskRepository.getDue(now, 2), hasSize(2));
    }

    @Test
    public void get_due_should_be_sorted() {
        Instant now = TimeHelper.truncatedInstantNow();
        IntStream.range(0, 100).forEach(i ->
                        taskRepository.createIfNotExists(new Execution(now.minusSeconds(new Random().nextInt(10000)), oneTimeTask.instance("id" + i)))
        );
        List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(100));

        List<Execution> sortedDue = new ArrayList<>(due);
        sortedDue.sort(Comparator.comparing(Execution::getExecutionTime));
        assertThat(due, is(sortedDue));
    }

    @Test
    public void get_due_should_not_include_previously_unresolved() {
        Instant now = TimeHelper.truncatedInstantNow();
        final OneTimeTask<Void> unresolved1 = TestTasks.oneTime("unresolved1", Void.class, TestTasks.DO_NOTHING);
        final OneTimeTask<Void> unresolved2 = TestTasks.oneTime("unresolved2", Void.class, TestTasks.DO_NOTHING);
        final OneTimeTask<Void> unresolved3 = TestTasks.oneTime("unresolved3", Void.class, TestTasks.DO_NOTHING);

        assertThat(taskResolver.getUnresolved(), hasSize(0));

        // 1
        taskRepository.createIfNotExists(new Execution(now, unresolved1.instance("id")));
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(taskResolver.getUnresolved(), hasSize(1));
        assertEquals(1, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));

        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(taskResolver.getUnresolved(), hasSize(1));
        assertEquals(1, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK),
            "Execution should not have have been in the ResultSet");

        // 1, 2
        taskRepository.createIfNotExists(new Execution(now, unresolved2.instance("id")));
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(taskResolver.getUnresolved(), hasSize(2));
        assertEquals(2, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));

        // 1, 2, 3
        taskRepository.createIfNotExists(new Execution(now, unresolved3.instance("id")));
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(taskResolver.getUnresolved(), hasSize(3));
        assertEquals(3, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));
    }

    @Test
    public void picked_executions_should_not_be_returned_as_due() {
        Instant now = TimeHelper.truncatedInstantNow();
        taskRepository.createIfNotExists(new Execution(now, oneTimeTask.instance("id1")));
        List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));

        taskRepository.pick(due.get(0), now);
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    }

    @Test
    public void picked_execution_should_have_information_about_which_scheduler_processes_it() {
        Instant now = TimeHelper.truncatedInstantNow();
        final TaskInstance<Void> instance = oneTimeTask.instance("id1");
        taskRepository.createIfNotExists(new Execution(now, instance));
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
        taskRepository.createIfNotExists(new Execution(now, instance));

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
        taskRepository.createIfNotExists(new Execution(now, instance));
        List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));

        Execution execution = due.get(0);
        final Optional<Execution> pickedExecution = taskRepository.pick(execution, now);
        final Instant nextExecutionTime = now.plus(Duration.ofMinutes(1));
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
        taskRepository.createIfNotExists(new Execution(now, instance));
        List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));

        Execution execution = due.get(0);
        final Optional<Execution> pickedExecution = taskRepository.pick(execution, now);
        final Instant nextExecutionTime = now.plus(Duration.ofMinutes(1));
        taskRepository.reschedule(pickedExecution.get(), nextExecutionTime, now.minusSeconds(1), now, 1);

        final Optional<Execution> nextExecution = taskRepository.getExecution(instance);
        assertTrue(nextExecution.isPresent());
        assertThat(nextExecution.get().consecutiveFailures, is(1));
    }

    @Test
    public void reschedule_should_update_data_if_specified() {
        Instant now = TimeHelper.truncatedInstantNow();
        final TaskInstance<Integer> instance = oneTimeTaskWithData.instance("id1", 1);
        taskRepository.createIfNotExists(new Execution(now, instance));

        Execution created = taskRepository.getExecution(instance).get();
        assertEquals(created.taskInstance.getData(), 1);

        final Instant nextExecutionTime = now.plus(Duration.ofMinutes(1));
        taskRepository.reschedule(created, nextExecutionTime, 2, now, null, 0);

        final Execution rescheduled = taskRepository.getExecution(instance).get();
        assertEquals(rescheduled.taskInstance.getData(), 2);
    }

    @Test
    public void test_get_failing_executions() {
        Instant now = TimeHelper.truncatedInstantNow();
        final TaskInstance<Void> instance = oneTimeTask.instance("id1");
        taskRepository.createIfNotExists(new Execution(now, instance));

        List<Execution> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));

        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

        taskRepository.reschedule(getSingleDueExecution(), now, now, null, 0);
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

        taskRepository.reschedule(getSingleDueExecution(), now, null, now, 1);
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofMinutes(1)), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofDays(1)), hasSize(1));

        taskRepository.reschedule(getSingleDueExecution(), now, now.minus(Duration.ofMinutes(1)), now, 1);
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofSeconds(1)), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofHours(1)), hasSize(0));
    }

    @Test
    public void get_scheduled_executions() {
        Instant now = TimeHelper.truncatedInstantNow();
        IntStream.range(0, 100).forEach(i ->
                taskRepository.createIfNotExists(new Execution(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.instance("id" + i)))
        );
        List<Execution> beforePick = new ArrayList();
        taskRepository.getScheduledExecutions(beforePick::add);
        assertThat(beforePick, hasSize(100));

        taskRepository.pick(beforePick.get(0), Instant.now());
        List<Execution> afterPick = new ArrayList<>();
        taskRepository.getScheduledExecutions(afterPick::add);
        assertThat(afterPick, hasSize(99));
    }

    @Test
    public void get_scheduled_by_task_name() {
        Instant now = TimeHelper.truncatedInstantNow();
        taskRepository.createIfNotExists(new Execution(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.instance("id" + 1)));
        taskRepository.createIfNotExists(new Execution(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.instance("id" + 2)));
        taskRepository.createIfNotExists(new Execution(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), alternativeOneTimeTask.instance("id" + 3)));

        List<Execution> scheduledByTaskName = new ArrayList<>();
        taskRepository.getScheduledExecutions(oneTimeTask.getName(), scheduledByTaskName::add);
        assertThat(scheduledByTaskName, hasSize(2));

        List<Execution> alternativeTasks = new ArrayList<>();
        taskRepository.getScheduledExecutions(alternativeOneTimeTask.getName(), alternativeTasks::add);
        assertThat(alternativeTasks, hasSize(1));

        List<Execution> empty = new ArrayList();
        taskRepository.getScheduledExecutions("non-existing", empty::add);
        assertThat(empty, empty());
    }

    @Test
    public void get_dead_executions_should_not_include_previously_unresolved() {
        Instant now = TimeHelper.truncatedInstantNow();

        // 1
        final Instant timeDied = now.minus(Duration.ofDays(5));
        createDeadExecution(oneTimeTask.instance("id1"), timeDied);

        TaskResolver taskResolverMissingTask = new TaskResolver(testableRegistry);
        JdbcTaskRepository repoMissingTask = new JdbcTaskRepository(DB.getDataSource(), DEFAULT_TABLE_NAME, taskResolverMissingTask, new SchedulerName.Fixed(SCHEDULER_NAME));

        assertThat(taskResolverMissingTask.getUnresolved(), hasSize(0));
        assertEquals(0, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));

        assertThat(repoMissingTask.getDeadExecutions(timeDied), hasSize(0));
        assertThat(taskResolverMissingTask.getUnresolved(), hasSize(1));
        assertEquals(1, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));

        assertThat(repoMissingTask.getDeadExecutions(timeDied), hasSize(0));
        assertThat(taskResolverMissingTask.getUnresolved(), hasSize(1));
        assertEquals(1, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));
    }

    private void createDeadExecution(TaskInstance<Void> taskInstance, Instant timeDied) {
        taskRepository.createIfNotExists(new Execution(timeDied, taskInstance));
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
        taskRepository.getScheduledExecutions(executions::add);
        return executions.get(0);
    }
}
