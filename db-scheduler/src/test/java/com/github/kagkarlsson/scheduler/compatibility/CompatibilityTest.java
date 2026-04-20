package com.github.kagkarlsson.scheduler.compatibility;

import static com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter.all;
import static com.github.kagkarlsson.scheduler.SchedulerBuilder.UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH;
import static com.github.kagkarlsson.scheduler.helper.TimeHelper.truncatedInstantNow;
import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import co.unruly.matchers.OptionalMatchers;
import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.ExecutionTimeAndId;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
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
import com.github.kagkarlsson.scheduler.helper.TimeHelper;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.SchedulableTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
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
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ConstantConditions")
public abstract class CompatibilityTest {

  private static final int POLLING_LIMIT = 10_000;
  private static final TaskDescriptor<String> ONETIME = TaskDescriptor.of("oneTime", String.class);
  private static final Instant anInstant = truncatedInstantNow();
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

  private JdbcCustomization resolveJdbcCustomization() {
    return getJdbcCustomization().orElse(new AutodetectJdbcCustomization(getDataSource()));
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
    SchedulerBuilder builder =
        Scheduler.create(getDataSource(), Lists.newArrayList(oneTime, recurring))
            .pollingInterval(Duration.ofMillis(10))
            .pollUsingFetchAndLockOnExecute(0, UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH)
            .heartbeatInterval(Duration.ofMillis(100))
            .schedulerName(new SchedulerName.Fixed("test"))
            .addSchedulerListener(testableListener)
            .commitWhenAutocommitDisabled(commitWhenAutocommitDisabled());
    getJdbcCustomization().ifPresent(builder::jdbcCustomization);
    Scheduler scheduler = builder.build();
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
  public void test_jdbc_limit_before_after_compatibility() {
    var repo = createJdbcTaskRepository(false);
    schedule(repo, ONETIME, "1", anInstant.plusSeconds(10));
    schedule(repo, ONETIME, "2", anInstant.plusSeconds(20));
    schedule(repo, ONETIME, "3", anInstant.plusSeconds(30));
    schedule(repo, ONETIME, "4", anInstant.plusSeconds(40));
    schedule(repo, ONETIME, "5", anInstant.plusSeconds(50));

    List<Execution> firstPage = repo.getScheduledExecutions(all().limit(3));
    assertThat(idsFrom(firstPage), contains("1", "2", "3"));

    var theSecond = firstPage.get(1); // id=2
    var theThird = firstPage.get(2); // id=3

    var beforePage =
        repo.getScheduledExecutions(
            all().before(ExecutionTimeAndId.from(toScheduled(theThird))).limit(10));

    assertThat(idsFrom(beforePage), contains("2", "1"));

    var afterPage =
        repo.getScheduledExecutions(
            all().after(ExecutionTimeAndId.from(toScheduled(theThird))).limit(10));
    assertThat(idsFrom(afterPage), contains("4", "5"));

    var theFourth = afterPage.get(0); // id=4

    var rangePage =
        repo.getScheduledExecutions(
            all()
                .after(ExecutionTimeAndId.from(toScheduled(theSecond)))
                .before(ExecutionTimeAndId.from(toScheduled(theFourth)))
                .limit(10));

    assertThat(idsFrom(rangePage), contains("3"));
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

  private ScheduledExecution<?> toScheduled(Execution execution) {
    return new ScheduledExecution<>(String.class, execution);
  }

  private static List<String> idsFrom(List<Execution> firstPage) {
    return firstPage.stream().map(Execution::getId).toList();
  }

  private void schedule(
      JdbcTaskRepository repo,
      TaskDescriptor<String> descriptor,
      String id,
      Instant executionTime) {
    repo.createIfNotExists(descriptor.instance(id).scheduledTo(executionTime.plusSeconds(10)));
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
    JdbcCustomization jdbcCustomization = resolveJdbcCustomization();
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

  /**
   * Verifies that with {@code alwaysPersistTimestampInUTC()} + plain {@code TIMESTAMP} columns
   * there is no drift across DST transitions. The JVM timezone is forced to a DST-observing zone
   * (Europe/Oslo) and instants in winter, summer, and straddling both DST boundaries are
   * round-tripped through the repository.
   *
   * <p>Regression test for kagkarlsson/db-scheduler#794.
   */
  @Test
  public void test_no_drift_when_storing_and_retrieving_instants() {
    TaskDescriptor<String> descriptor = TaskDescriptor.of("utcTest", String.class);
    OneTimeTask<String> task = Tasks.oneTime(descriptor).execute((instance, ctx) -> {});

    TaskResolver taskResolver =
        new TaskResolver(SchedulerListeners.NOOP, new SystemClock(), List.of());
    taskResolver.addTask(task);

    JdbcTaskRepository taskRepo =
        new JdbcTaskRepository(
            getDataSource(),
            commitWhenAutocommitDisabled(),
            resolveJdbcCustomization(),
            DEFAULT_TABLE_NAME,
            taskResolver,
            new SchedulerName.Fixed("scheduler1"),
            false,
            new SystemClock());

    TimeZone originalTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Oslo"));

    try {
      logSessionOffset();

      // Winter: Europe/Oslo is CET (UTC+1)
      assertRoundTrip(
          task,
          taskRepo,
          "winter",
          Instant.parse("2020-01-15T11:00:00Z")); // MariaDB103CompatibilityTest fails here

      // Summer: Europe/Oslo is CEST (UTC+2)
      assertRoundTrip(task, taskRepo, "summer", Instant.parse("2020-07-15T11:00:00Z"));

      // Spring-forward boundary: 2020-03-29 local 02:00 CET -> 03:00 CEST (01:00 UTC).
      assertRoundTrip(task, taskRepo, "spring_before", Instant.parse("2020-03-29T00:30:00Z"));
      assertRoundTrip(task, taskRepo, "spring_after", Instant.parse("2020-03-29T01:30:00Z"));

      // Fall-back boundary: 2020-10-25 local 03:00 CEST -> 02:00 CET (01:00 UTC).
      // The local 02:00-03:00 hour is ambiguous — most likely place for fold-related drift.
      assertRoundTrip(task, taskRepo, "fall_before", Instant.parse("2020-10-25T00:30:00Z"));
      assertRoundTrip(task, taskRepo, "fall_after", Instant.parse("2020-10-25T01:30:00Z"));

    } finally {
      TimeZone.setDefault(originalTz);
    }
  }

  private void logSessionOffset() {
    OffsetDateTime now = readDbCurrentTimestamp();
    OffsetDateTime jvmNow = OffsetDateTime.now();
    LOG.info(
        "DB session offset: {} (CURRENT_TIMESTAMP = {}) | JVM offset: {} ({})",
        now == null ? "?" : now.getOffset(),
        now,
        jvmNow.getOffset(),
        TimeZone.getDefault().getID());
  }

  private OffsetDateTime readDbCurrentTimestamp() {
    for (String sql :
        new String[] {
          "SELECT CURRENT_TIMESTAMP",
          "SELECT CURRENT_TIMESTAMP FROM DUAL",
          "SELECT SYSDATETIMEOFFSET()"
        }) {
      try (Connection c = getDataSource().getConnection();
          Statement s = c.createStatement();
          ResultSet rs = s.executeQuery(sql)) {
        rs.next();
        return rs.getObject(1, OffsetDateTime.class);
      } catch (Exception ignored) {
        // try next variant
      }
    }
    return null;
  }

  private void assertRoundTrip(
      OneTimeTask<String> task,
      JdbcTaskRepository taskRepo,
      String instanceId,
      Instant storedInstant) {

    TaskInstance<String> instance = task.instance(instanceId);
    taskRepo.createIfNotExists(SchedulableInstance.of(instance, storedInstant));

    Instant roundTripped = taskRepo.getExecution(instance).get().executionTime;
    assertThat(
        "round-tripped instant should equal stored instant for " + storedInstant,
        roundTripped,
        is(storedInstant));
  }

  private void doJDBCRepositoryCompatibilityTestUsingData(String data) {
    SystemClock clock = new SystemClock();
    TaskResolver taskResolver = new TaskResolver(SchedulerListeners.NOOP, clock, List.of());
    taskResolver.addTask(oneTime);

    DataSource dataSource = getDataSource();
    JdbcCustomization jdbcCustomization = resolveJdbcCustomization();
    final JdbcTaskRepository jdbcTaskRepository =
        new JdbcTaskRepository(
            dataSource,
            commitWhenAutocommitDisabled(),
            jdbcCustomization,
            DEFAULT_TABLE_NAME,
            taskResolver,
            new SchedulerName.Fixed("scheduler1"),
            false,
            clock);

    final JdbcTaskRepository jdbcTaskRepositoryWithPriority =
        new JdbcTaskRepository(
            dataSource,
            commitWhenAutocommitDisabled(),
            jdbcCustomization,
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
            resolveJdbcCustomization(),
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

  @RepeatedTest(5)
  void test_compatibility_manual_scheduler() {
    final SettableClock clock = new SettableClock();
    final TestHelper.ManualSchedulerBuilder builder =
        new TestHelper.ManualSchedulerBuilder(getDataSource(), List.of(oneTime));
    builder.clock(clock).commitWhenAutocommitDisabled(commitWhenAutocommitDisabled());
    getJdbcCustomization().ifPresent(builder::jdbcCustomization);
    final ManualScheduler scheduler = (ManualScheduler) builder.build();

    scheduler.schedule(oneTime.instance("1"), TimeHelper.truncated(clock.now()));

    scheduler.runAnyDueExecutions();

    assertThat(delayingHandlerOneTime.timesExecuted.get(), is(1));
  }

  private JdbcTaskRepository createRepositoryForForZone(
      TaskResolver defaultTaskResolver, String zoneId) {

    ZoneSpecificJdbcCustomization jdbcCustomization =
        new ZoneSpecificJdbcCustomization(
            resolveJdbcCustomization(),
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
