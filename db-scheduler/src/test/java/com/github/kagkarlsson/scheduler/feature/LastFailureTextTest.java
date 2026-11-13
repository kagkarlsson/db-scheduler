package com.github.kagkarlsson.scheduler.feature;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.FailureTextFormatter;
import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class LastFailureTextTest {

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  private final AtomicBoolean fail = new AtomicBoolean(true);
  private SettableClock clock;
  private RecurringTask<Void> recurring;
  private OneTimeTask<Void> oneTime;

  @BeforeEach
  void setUp() {
    clock = new SettableClock();
    recurring =
        Tasks.recurring("recurring", FixedDelay.ofSeconds(1))
            .execute((inst, ctx) -> failIfSet("recurring failed"));
    oneTime =
        Tasks.oneTime("one-time")
            .onFailure(
                FailureHandler.<Void>maxRetries(1)
                    .retryEvery(Duration.ofSeconds(1))
                    .thenDeactivate(State.FAILED))
            .execute((inst, ctx) -> failIfSet("one-time failed"));
  }

  @Test
  void recurring_failure_stores_stack_trace_and_success_keeps_it() {
    var scheduler = start(FailureTextFormatter.STACK_TRACE);
    var instance = recurring.instance(RecurringTask.INSTANCE);
    scheduler.schedule(instance, clock.now());

    runOnce(scheduler);
    var failed = get(scheduler, instance);
    assertThat(failed.getConsecutiveFailures()).isEqualTo(1);
    assertThat(failed.getLastFailureText())
        .startsWith("java.lang.RuntimeException: recurring failed\n\tat ")
        .contains("Caused by: java.lang.IllegalStateException: root cause")
        .contains("LastFailureTextTest");

    fail.set(false);
    runOnce(scheduler);
    var succeeded = get(scheduler, instance);
    assertThat(succeeded.getConsecutiveFailures()).isZero();
    assertThat(succeeded.getLastFailure()).isEqualTo(failed.getLastFailure());
    assertThat(succeeded.getLastFailureText()).isEqualTo(failed.getLastFailureText());

    scheduler.reschedule(instance, clock.now().plusSeconds(60));
    assertThat(get(scheduler, instance).getLastFailureText()).isNull();
  }

  @Test
  void deactivated_to_failed_keeps_text_and_reactivate_clears_it() {
    var scheduler = start(FailureTextFormatter.MESSAGE_ONLY);
    var instance = oneTime.instance("1");
    scheduler.schedule(instance, clock.now());

    runOnce(scheduler); // fails, retried
    runOnce(scheduler); // fails, max retries exceeded -> FAILED
    var deactivated = get(scheduler, instance);
    assertThat(deactivated.getState()).isEqualTo(State.FAILED);
    assertThat(deactivated.getLastFailureText())
        .isEqualTo(
            "java.lang.RuntimeException: one-time failed\n"
                + "Caused by: java.lang.IllegalStateException: root cause");

    scheduler.reactivate(instance, clock.now());
    assertThat(get(scheduler, instance).getLastFailureText()).isNull();
  }

  @Test
  void formatter_none_stores_nothing() {
    var scheduler = start(FailureTextFormatter.NONE);
    var instance = recurring.instance(RecurringTask.INSTANCE);
    scheduler.schedule(instance, clock.now());

    runOnce(scheduler);
    var failed = get(scheduler, instance);
    assertThat(failed.getConsecutiveFailures()).isEqualTo(1);
    assertThat(failed.getLastFailureText()).isNull();
  }

  @Test
  void dead_execution_gets_generated_text() {
    var scheduler = start(FailureTextFormatter.NONE);
    var repository =
        new JdbcTaskRepository(
            postgres.getDataSource(),
            false,
            JdbcTaskRepository.DEFAULT_TABLE_NAME,
            new TaskResolver(SchedulerListeners.NOOP, clock, List.of(recurring, oneTime)),
            new SchedulerName.Fixed("crashed-node"),
            false,
            clock);
    var instance = oneTime.instance("dead");
    var now = clock.now();
    repository.createIfNotExists(SchedulableInstance.of(instance, now));
    var picked = repository.pick(repository.getExecution(instance).orElseThrow(), now);
    repository.updateHeartbeat(picked.orElseThrow(), now.minus(Duration.ofHours(1)));

    scheduler.runDeadExecutionDetection();

    var revived = get(scheduler, instance);
    assertThat(revived.isPicked()).isFalse();
    assertThat(revived.getConsecutiveFailures()).isEqualTo(1);
    assertThat(revived.getLastFailureText())
        .startsWith("Dead execution: picked by 'crashed-node', last heartbeat ")
        .endsWith("(detected by 'manual')");
  }

  private void failIfSet(String message) {
    if (fail.get()) {
      throw new RuntimeException(message, new IllegalStateException("root cause"));
    }
  }

  private ManualScheduler start(FailureTextFormatter formatter) {
    var builder = TestHelper.createManualScheduler(postgres.getDataSource(), recurring, oneTime);
    builder.clock(clock).failureTextFormatter(formatter);
    var scheduler = builder.build();
    scheduler.start();
    return scheduler;
  }

  private void runOnce(ManualScheduler scheduler) {
    scheduler.tick(Duration.ofSeconds(2));
    scheduler.runAnyDueExecutions();
  }

  private static ScheduledExecution<Object> get(
      ManualScheduler scheduler, TaskInstance<?> instance) {
    return scheduler.getScheduledExecution((TaskInstanceId) instance).orElseThrow();
  }
}
