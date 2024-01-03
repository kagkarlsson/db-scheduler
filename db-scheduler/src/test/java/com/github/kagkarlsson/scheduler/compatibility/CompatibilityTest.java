package com.github.kagkarlsson.scheduler.compatibility;

import static com.github.kagkarlsson.scheduler.SchedulerBuilder.UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH;
import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import co.unruly.matchers.OptionalMatchers;
import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.SystemClock;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.TestTasks.DoNothingHandler;
import com.github.kagkarlsson.scheduler.helper.ExecutionCompletedCondition;
import com.github.kagkarlsson.scheduler.helper.TestableRegistry;
import com.github.kagkarlsson.scheduler.helper.TimeHelper;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.SchedulableTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import javax.sql.DataSource;
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ConstantConditions")
public abstract class CompatibilityTest {

  private static final int POLLING_LIMIT = 10_000;
  private final boolean supportsSelectForUpdate;

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  private TestTasks.CountingHandler<String> delayingHandlerOneTime;
  private TestTasks.CountingHandler<Void> delayingHandlerRecurring;
  private OneTimeTask<String> oneTime;
  private RecurringTask<Void> recurring;
  private RecurringTask<Integer> recurringWithData;

  private TestableRegistry testableRegistry;
  private ExecutionCompletedCondition completed12Condition;

  public CompatibilityTest(boolean supportsSelectForUpdate) {
    this.supportsSelectForUpdate = supportsSelectForUpdate;
  }

  public abstract DataSource getDataSource();

  public Optional<JdbcCustomization> getJdbcCustomization() {
    return Optional.empty();
  }

  public abstract boolean commitWhenAutocommitDisabled();

  @BeforeEach
  public void setUp() {
    delayingHandlerOneTime = new TestTasks.CountingHandler<>(Duration.ofMillis(200));
    delayingHandlerRecurring = new TestTasks.CountingHandler<>(Duration.ofMillis(200));

    oneTime = TestTasks.oneTimeWithType("oneTime", String.class, delayingHandlerOneTime);
    recurring = TestTasks.recurring("recurring", FixedDelay.ofMillis(10), delayingHandlerRecurring);
    recurringWithData =
        TestTasks.recurringWithData(
            "recurringWithData",
            Integer.class,
            0,
            FixedDelay.ofMillis(10),
            new DoNothingHandler<>());

    completed12Condition = new ExecutionCompletedCondition(12);
    testableRegistry = new TestableRegistry(false, singletonList(completed12Condition));
  }

  @AfterEach
  public void clearTables() {
    assertTimeoutPreemptively(Duration.ofSeconds(20), () -> DbUtils.clearTables(getDataSource()));
  }

  @Test
  public void test_compatibility_fetch_and_lock_on_execute() {
    Scheduler scheduler =
        Scheduler.create(getDataSource(), Lists.newArrayList(oneTime, recurring))
            .pollingInterval(Duration.ofMillis(10))
            .pollUsingFetchAndLockOnExecute(0, UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH)
            .heartbeatInterval(Duration.ofMillis(100))
            .schedulerName(new SchedulerName.Fixed("test"))
            .statsRegistry(testableRegistry)
            .commitWhenAutocommitDisabled(commitWhenAutocommitDisabled())
            .build();
    stopScheduler.register(scheduler);

    testCompatibilityForSchedulerConfiguration(scheduler);
  }

  @Test
  public void test_compatibility_lock_and_fetch() {
    if (!supportsSelectForUpdate) {
      return;
    }

    SchedulerBuilder builder =
        Scheduler.create(getDataSource(), Lists.newArrayList(oneTime, recurring))
            .pollingInterval(Duration.ofMillis(10))
            .pollUsingLockAndFetch(0.5, 1.0)
            .heartbeatInterval(Duration.ofMillis(500))
            .schedulerName(new SchedulerName.Fixed("test"))
            .statsRegistry(testableRegistry)
            .commitWhenAutocommitDisabled(commitWhenAutocommitDisabled());

    getJdbcCustomization().ifPresent(builder::jdbcCustomization);
    Scheduler scheduler = builder.build();

    stopScheduler.register(scheduler);

    testCompatibilityForSchedulerConfiguration(scheduler);
  }

  private void testCompatibilityForSchedulerConfiguration(Scheduler scheduler) {
    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          scheduler.schedule(oneTime.instance("id1"), Instant.now());
          scheduler.schedule(oneTime.instance("id1"), Instant.now()); // duplicate
          scheduler.schedule(recurring.instance("id1"), Instant.now());
          scheduler.schedule(recurring.instance("id2"), Instant.now());
          scheduler.schedule(recurring.instance("id3"), Instant.now());
          scheduler.schedule(recurring.instance("id4"), Instant.now());

          scheduler.start();
          completed12Condition.waitFor();
          scheduler.stop();
          testableRegistry.assertNoFailures();

          assertThat(delayingHandlerRecurring.timesExecuted.get(), greaterThan(10));
          assertThat(delayingHandlerOneTime.timesExecuted.get(), is(1));
        });
  }

  @Test
  public void test_jdbc_repository_compatibility() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(20),
        () -> {
          doJDBCRepositoryCompatibilityTestUsingData(null);
        });
  }

  @Test
  public void test_jdbc_repository_compatibility_with_data() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(20),
        () -> {
          doJDBCRepositoryCompatibilityTestUsingData("my data");
        });
  }

  @Test
  public void test_jdbc_repository_select_for_update_compatibility() {
    if (!supportsSelectForUpdate) {
      return;
    }

    TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, new ArrayList<>());
    taskResolver.addTask(oneTime);

    DataSource dataSource = getDataSource();
    JdbcCustomization jdbcCustomization =
        getJdbcCustomization().orElse(new AutodetectJdbcCustomization(dataSource));
    final JdbcTaskRepository jdbcTaskRepository =
        new JdbcTaskRepository(
            dataSource,
            commitWhenAutocommitDisabled(),
            jdbcCustomization,
            DEFAULT_TABLE_NAME,
            taskResolver,
            new SchedulerName.Fixed("scheduler1"),
            new SystemClock());

    final Instant now = TimeHelper.truncatedInstantNow();

    jdbcTaskRepository.createIfNotExists(
        SchedulableInstance.of(oneTime.instance("future1"), now.plusSeconds(10)));
    jdbcTaskRepository.createIfNotExists(
        new SchedulableTaskInstance<>(oneTime.instance("id1"), now));
    List<Execution> picked = jdbcTaskRepository.lockAndGetDue(now, POLLING_LIMIT);
    assertThat(picked, IsCollectionWithSize.hasSize(1));

    assertThat(jdbcTaskRepository.pick(picked.get(0), now), OptionalMatchers.empty());
  }

  @Test
  public void test_persistent_instant() {
    TaskResolver defaultTaskResolver = new TaskResolver(StatsRegistry.NOOP, new ArrayList<>());
    defaultTaskResolver.addTask(oneTime);

    JdbcTaskRepository winterTaskRepo =
      new JdbcTaskRepository(
        getDataSource(),
        commitWhenAutocommitDisabled(),
        new ZoneSpecificJdbcCustomization(getJdbcCustomization().orElse(new AutodetectJdbcCustomization(getDataSource())),
          GregorianCalendar.getInstance(TimeZone.getTimeZone("CET"))
        ),
        DEFAULT_TABLE_NAME,
        defaultTaskResolver,
        new SchedulerName.Fixed("scheduler1"),
        new SystemClock());

    JdbcTaskRepository summerTaskRepo =
      new JdbcTaskRepository(
        getDataSource(),
        commitWhenAutocommitDisabled(),
        new ZoneSpecificJdbcCustomization(getJdbcCustomization().orElse(new AutodetectJdbcCustomization(getDataSource())),
          GregorianCalendar.getInstance(TimeZone.getTimeZone("CEST"))
        ),
        DEFAULT_TABLE_NAME,
        defaultTaskResolver,
        new SchedulerName.Fixed("scheduler1"),
        new SystemClock());


    final Instant now = TimeHelper.truncatedInstantNow();
    Instant noonFirstJan = Instant.parse("2020-01-01T12:00:00.00Z");

    TaskInstance<String> instance1 = oneTime.instance("future1");
    winterTaskRepo.createIfNotExists(SchedulableInstance.of(instance1, noonFirstJan));

    assertThat(winterTaskRepo.getExecution(instance1).get().executionTime,
      is (summerTaskRepo.getExecution(instance1).get().executionTime));
  }

  private void doJDBCRepositoryCompatibilityTestUsingData(String data) {
    TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, new ArrayList<>());
    taskResolver.addTask(oneTime);

    DataSource dataSource = getDataSource();
    final JdbcTaskRepository jdbcTaskRepository =
        new JdbcTaskRepository(
            dataSource,
            commitWhenAutocommitDisabled(),
            DEFAULT_TABLE_NAME,
            taskResolver,
            new SchedulerName.Fixed("scheduler1"),
            new SystemClock());

    final Instant now = TimeHelper.truncatedInstantNow();

    final TaskInstance<String> taskInstance = oneTime.instance("id1", data);
    final SchedulableTaskInstance<String> newExecution =
        new SchedulableTaskInstance<>(taskInstance, now);
    jdbcTaskRepository.createIfNotExists(newExecution);
    Execution storedExecution = (jdbcTaskRepository.getExecution(taskInstance)).get();
    assertThat(storedExecution.getExecutionTime(), is(now));

    final List<Execution> due = jdbcTaskRepository.getDue(now, POLLING_LIMIT);
    assertThat(due, hasSize(1));
    final Optional<Execution> pickedExecution = jdbcTaskRepository.pick(due.get(0), now);
    assertThat(pickedExecution.isPresent(), is(true));

    assertThat(jdbcTaskRepository.getDue(now, POLLING_LIMIT), hasSize(0));

    jdbcTaskRepository.updateHeartbeat(pickedExecution.get(), now.plusSeconds(1));
    assertThat(jdbcTaskRepository.getDeadExecutions(now.plus(Duration.ofDays(1))), hasSize(1));

    jdbcTaskRepository.reschedule(
        pickedExecution.get(), now.plusSeconds(1), now.minusSeconds(1), now.minusSeconds(1), 0);
    assertThat(jdbcTaskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    assertThat(
        jdbcTaskRepository.getDue(now.plus(Duration.ofMinutes(1)), POLLING_LIMIT), hasSize(1));

    final Optional<Execution> rescheduled = jdbcTaskRepository.getExecution(taskInstance);
    assertThat(rescheduled.isPresent(), is(true));
    assertThat(rescheduled.get().lastHeartbeat, nullValue());
    assertThat(rescheduled.get().isPicked(), is(false));
    assertThat(rescheduled.get().pickedBy, nullValue());
    assertThat(rescheduled.get().taskInstance.getData(), is(data));
    jdbcTaskRepository.remove(rescheduled.get());
  }

  @Test
  public void test_jdbc_repository_compatibility_set_data() {
    TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, new ArrayList<>());
    taskResolver.addTask(recurringWithData);

    DataSource dataSource = getDataSource();
    final JdbcTaskRepository jdbcTaskRepository =
        new JdbcTaskRepository(
            dataSource,
            commitWhenAutocommitDisabled(),
            DEFAULT_TABLE_NAME,
            taskResolver,
            new SchedulerName.Fixed("scheduler1"),
            new SystemClock());

    final Instant now = TimeHelper.truncatedInstantNow();

    final TaskInstance<Integer> taskInstance = recurringWithData.instance("id1", 1);
    final SchedulableTaskInstance<Integer> newExecution =
        new SchedulableTaskInstance<>(taskInstance, now);

    jdbcTaskRepository.createIfNotExists(newExecution);

    Execution round1 = jdbcTaskRepository.getExecution(taskInstance).get();
    assertEquals(round1.taskInstance.getData(), 1);
    jdbcTaskRepository.reschedule(
        round1, now.plusSeconds(1), 2, now.minusSeconds(1), now.minusSeconds(1), 0);

    Execution round2 = jdbcTaskRepository.getExecution(taskInstance).get();
    assertEquals(round2.taskInstance.getData(), 2);

    jdbcTaskRepository.reschedule(
        round2, now.plusSeconds(2), null, now.minusSeconds(2), now.minusSeconds(2), 0);
    Execution round3 = jdbcTaskRepository.getExecution(taskInstance).get();
    assertNull(round3.taskInstance.getData());
  }

  private void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      LoggerFactory.getLogger(CompatibilityTest.class).info("Interrupted");
    }
  }
}
