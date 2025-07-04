package com.github.kagkarlsson.scheduler.compatibility;

import static com.github.kagkarlsson.scheduler.SchedulerBuilder.UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH;
import static com.github.kagkarlsson.scheduler.helper.TimeHelper.truncatedInstantNow;
import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import co.unruly.matchers.OptionalMatchers;
import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.SystemClock;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.TestTasks.DoNothingHandler;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.helper.ExecutionCompletedCondition;
import com.github.kagkarlsson.scheduler.helper.TestableListener;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.jdbc.PostgreSqlJdbcCustomization;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.SchedulableTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Function;
import javax.sql.DataSource;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ConstantConditions")
public abstract class CompatibilityTest {

  private static final int POLLING_LIMIT = 10_000;
  private static final TaskDescriptor<String> ONETIME = TaskDescriptor.of("oneTime", String.class);
  private Logger LOG = LoggerFactory.getLogger(getClass());
  private final boolean supportsSelectForUpdate;
  private final boolean shouldHavePersistentTimezone;

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  private TestTasks.CountingHandler<String> delayingHandlerOneTime;
  private TestTasks.CountingHandler<Void> delayingHandlerRecurring;
  private OneTimeTask<String> oneTime;
  private RecurringTask<Void> recurring;
  private RecurringTask<Integer> recurringWithData;

  private TestableListener testableListener;
  private ExecutionCompletedCondition completed12Condition;

  public CompatibilityTest(boolean supportsSelectForUpdate, boolean shouldHavePersistentTimezone) {
    this.supportsSelectForUpdate = supportsSelectForUpdate;
    this.shouldHavePersistentTimezone = shouldHavePersistentTimezone;
  }

  public abstract DataSource getDataSource();

  public Optional<JdbcCustomization> getJdbcCustomization() {
    return Optional.empty();
  }

  public Optional<JdbcCustomization> getJdbcCustomizationForUTCTimestampTest() {
    return Optional.empty();
  }

  public abstract boolean commitWhenAutocommitDisabled();

  @BeforeEach
  public void setUp() {
    delayingHandlerOneTime = new TestTasks.CountingHandler<>(Duration.ofMillis(200));
    delayingHandlerRecurring = new TestTasks.CountingHandler<>(Duration.ofMillis(200));

    oneTime =
        TestTasks.oneTimeWithType(
            ONETIME.getTaskName(), ONETIME.getDataClass(), delayingHandlerOneTime);
    recurring = TestTasks.recurring("recurring", FixedDelay.ofMillis(10), delayingHandlerRecurring);
    recurringWithData =
        TestTasks.recurringWithData(
            "recurringWithData",
            Integer.class,
            0,
            FixedDelay.ofMillis(10),
            new DoNothingHandler<>());

    completed12Condition = new ExecutionCompletedCondition(12);
    testableListener = new TestableListener(false, singletonList(completed12Condition));
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
            .addSchedulerListener(testableListener)
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
            .addSchedulerListener(testableListener)
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
          testableListener.assertNoFailures();

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

    final JdbcTaskRepository jdbcTaskRepository = createJdbcTaskRepository(false);

    final Instant now = truncatedInstantNow();

    jdbcTaskRepository.createIfNotExists(
        ONETIME.instance("future1").scheduledTo(now.plusSeconds(10)));
    jdbcTaskRepository.createIfNotExists(ONETIME.instance("id1").scheduledTo(now));
    List<Execution> picked = jdbcTaskRepository.lockAndGetDue(now, POLLING_LIMIT);
    assertThat(picked, IsCollectionWithSize.hasSize(1));

    assertThat(jdbcTaskRepository.pick(picked.get(0), now), OptionalMatchers.empty());
  }

  @Test
  public void test_jdbc_repository_get_due_correct_order_with_priority() {
    assertCorrectOrder(true, (r) -> r.getDue(truncatedInstantNow(), POLLING_LIMIT));
  }

  @Test
  public void test_jdbc_repository_get_due_correct_order_without_priority() {
    assertCorrectOrder(false, (r) -> r.getDue(truncatedInstantNow(), POLLING_LIMIT));
  }

  @Test
  public void test_jdbc_repository_lock_and_get_due_correct_order_with_priority() {
    if (!supportsSelectForUpdate) {
      return;
    }

    assertCorrectOrder(true, (r) -> r.lockAndGetDue(truncatedInstantNow(), POLLING_LIMIT));
  }

  @Test
  public void test_jdbc_repository_lock_and_get_due_correct_order_without_priority() {
    if (!supportsSelectForUpdate) {
      return;
    }

    assertCorrectOrder(false, (r) -> r.lockAndGetDue(truncatedInstantNow(), POLLING_LIMIT));
  }

  private void assertCorrectOrder(
      boolean orderByPriority, Function<JdbcTaskRepository, List<Execution>> methodUnderTest) {
    final JdbcTaskRepository jdbcTaskRepository = createJdbcTaskRepository(orderByPriority);
    final Instant now = truncatedInstantNow();

    jdbcTaskRepository.createIfNotExists(
        ONETIME.instance("1").priority(1).scheduledTo(now.minusSeconds(10)));
    jdbcTaskRepository.createIfNotExists(
        ONETIME.instance("2").priority(2).scheduledTo(now.minusSeconds(30)));
    jdbcTaskRepository.createIfNotExists(
        ONETIME.instance("3").priority(3).scheduledTo(now.minusSeconds(20)));

    List<Execution> picked = methodUnderTest.apply(jdbcTaskRepository);
    List<String> ids = picked.stream().map(Execution::getId).collect(toList());

    if (orderByPriority) {
      assertThat("Contained: " + ids, ids, Matchers.contains("3", "2", "1"));
    } else {
      assertThat("Contained: " + ids, ids, Matchers.contains("2", "3", "1"));
    }
  }

  private JdbcTaskRepository createJdbcTaskRepository(boolean orderByPriority) {
    SystemClock clock = new SystemClock();
    TaskResolver taskResolver = new TaskResolver(SchedulerListeners.NOOP, clock, new ArrayList<>());
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
            orderByPriority,
            clock);
    return jdbcTaskRepository;
  }

  @Test
  public void test_has_peristent_time_zone() {
    if (!shouldHavePersistentTimezone) {
      return;
    }

    TaskResolver defaultTaskResolver =
        new TaskResolver(SchedulerListeners.NOOP, new SystemClock(), List.of());
    defaultTaskResolver.addTask(oneTime);

    JdbcTaskRepository winterTaskRepo = createRepositoryForForZone(defaultTaskResolver, "CET");
    JdbcTaskRepository summerTaskRepo = createRepositoryForForZone(defaultTaskResolver, "CEST");

    Instant noonFirstJan = Instant.parse("2020-01-01T12:00:00.00Z");

    TaskInstance<String> instance1 = oneTime.instance("future1");
    winterTaskRepo.createIfNotExists(SchedulableInstance.of(instance1, noonFirstJan));

    assertThat(
        winterTaskRepo.getExecution(instance1).get().executionTime,
        is(summerTaskRepo.getExecution(instance1).get().executionTime));
  }

  private void doJDBCRepositoryCompatibilityTestUsingData(String data) {
    SystemClock clock = new SystemClock();
    TaskResolver taskResolver = new TaskResolver(SchedulerListeners.NOOP, clock, List.of());
    taskResolver.addTask(oneTime);

    DataSource dataSource = getDataSource();
    final JdbcTaskRepository jdbcTaskRepository =
        new JdbcTaskRepository(
            dataSource,
            commitWhenAutocommitDisabled(),
            DEFAULT_TABLE_NAME,
            taskResolver,
            new SchedulerName.Fixed("scheduler1"),
            false,
            clock);

    final JdbcTaskRepository jdbcTaskRepositoryWithPriority =
        new JdbcTaskRepository(
            dataSource,
            commitWhenAutocommitDisabled(),
            DEFAULT_TABLE_NAME,
            taskResolver,
            new SchedulerName.Fixed("scheduler1"),
            true,
            clock);

    final Instant now = truncatedInstantNow();

    final TaskInstance<String> taskInstance = oneTime.instance("id1", data);
    final SchedulableTaskInstance<String> newExecution =
        new SchedulableTaskInstance<>(taskInstance, now);
    jdbcTaskRepository.createIfNotExists(newExecution);
    Execution storedExecution = (jdbcTaskRepository.getExecution(taskInstance)).get();
    assertThat(storedExecution.getExecutionTime(), is(now));

    // priority=true
    assertThat(jdbcTaskRepositoryWithPriority.getDue(now, POLLING_LIMIT), hasSize(1));

    // priority=false
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
    SystemClock clock = new SystemClock();
    TaskResolver taskResolver = new TaskResolver(SchedulerListeners.NOOP, clock, List.of());
    taskResolver.addTask(recurringWithData);

    DataSource dataSource = getDataSource();
    final JdbcTaskRepository jdbcTaskRepository =
        new JdbcTaskRepository(
            dataSource,
            commitWhenAutocommitDisabled(),
            DEFAULT_TABLE_NAME,
            taskResolver,
            new SchedulerName.Fixed("scheduler1"),
            false,
            clock);

    final Instant now = truncatedInstantNow();

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

  /**
   * Very uncertain on how to verify that the timestamp is stored in UTC. The asserts in this test
   * will likely always work, but at least the code for storing in UTC is exercised.
   */
  @Test
  public void test_jdbc_customization_supports_timestamps_in_utc() {
    Optional<JdbcCustomization> jdbcCustomization = getJdbcCustomizationForUTCTimestampTest();

    if (jdbcCustomization.isEmpty()) {
      LOG.info(
          "Skipping test_jdbc_customization_supports_timestamps_in_utc since to JdbcCustomization is spesified.");
      return;
    }
    LOG.info(
        "Running  test_jdbc_customization_supports_timestamps_in_utc for: "
            + jdbcCustomization.get().getName());

    TaskResolver defaultTaskResolver =
        new TaskResolver(SchedulerListeners.NOOP, new SystemClock(), List.of());
    defaultTaskResolver.addTask(oneTime);

    JdbcTaskRepository taskRepo =
        createRepositoryForJdbcCustomization(defaultTaskResolver, jdbcCustomization.get());

    ZonedDateTime zonedDateTime =
        ZonedDateTime.of(2020, 1, 1, 12, 0, 0, 0, ZoneId.of("Europe/Oslo"));

    TaskInstance<String> instance1 = oneTime.instance("future1");
    taskRepo.createIfNotExists(SchedulableInstance.of(instance1, zonedDateTime.toInstant()));

    assertThat(taskRepo.getExecution(instance1).get().executionTime, is(zonedDateTime.toInstant()));
  }

  @Test
  void test_compatibility_test_helper() {
    final SettableClock clock = new SettableClock();
    final ManualScheduler scheduler =
        (ManualScheduler)
            new TestHelper.ManualSchedulerBuilder(getDataSource(), List.of(oneTime))
                .clock(clock)
                .jdbcCustomization(new PostgreSqlJdbcCustomization(true, true))
                .commitWhenAutocommitDisabled(commitWhenAutocommitDisabled())
                .build();

    scheduler.schedule(oneTime.instance("1"), clock.now());
    scheduler.runAnyDueExecutions();

    assertThat(delayingHandlerOneTime.timesExecuted.get(), is(1));
  }

  private JdbcTaskRepository createRepositoryForForZone(
      TaskResolver defaultTaskResolver, String zoneId) {

    ZoneSpecificJdbcCustomization jdbcCustomization =
        new ZoneSpecificJdbcCustomization(
            getJdbcCustomization().orElse(new AutodetectJdbcCustomization(getDataSource())),
            GregorianCalendar.getInstance(TimeZone.getTimeZone(zoneId)));
    return createRepositoryForJdbcCustomization(defaultTaskResolver, jdbcCustomization);
  }

  private JdbcTaskRepository createRepositoryForJdbcCustomization(
      TaskResolver defaultTaskResolver, JdbcCustomization jdbcCustomization) {
    return new JdbcTaskRepository(
        getDataSource(),
        commitWhenAutocommitDisabled(),
        jdbcCustomization,
        DEFAULT_TABLE_NAME,
        defaultTaskResolver,
        new SchedulerName.Fixed("scheduler1"),
        false,
        new SystemClock());
  }
}
