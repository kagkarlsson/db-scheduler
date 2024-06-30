package com.github.kagkarlsson.scheduler;

import static com.github.kagkarlsson.scheduler.SchedulerClient.Builder.create;
import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.unruly.matchers.OptionalMatchers;
import com.github.kagkarlsson.scheduler.TestTasks.SavingHandler;
import com.github.kagkarlsson.scheduler.serializer.JavaSerializer;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.VoidExecutionHandler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class SchedulerClientTest {

  @RegisterExtension public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

  private ManualScheduler scheduler;
  private SettableClock settableClock;

  private TestTasks.CountingHandler<Void> onetimeTaskHandlerA;
  private OneTimeTask<Void> oneTimeTaskA;

  private TestTasks.CountingHandler<Void> onetimeTaskHandlerB;
  private OneTimeTask<Void> oneTimeTaskB;

  private ScheduleAnotherTaskHandler<Void> scheduleAnother;
  private OneTimeTask<Void> scheduleAnotherTask;

  private SavingHandler<String> savingHandler;
  private OneTimeTask<String> savingTask;
  private OneTimeTask<Integer> oneTimeTaskC;
  private VoidExecutionHandler<Integer> onetimeTaskHandlerC;

  @BeforeEach
  public void setUp() {
    settableClock = new SettableClock();

    onetimeTaskHandlerA = new TestTasks.CountingHandler<>();
    oneTimeTaskA = TestTasks.oneTime("OneTimeA", Void.class, onetimeTaskHandlerA);

    oneTimeTaskB = TestTasks.oneTime("OneTimeB", Void.class, onetimeTaskHandlerB);
    onetimeTaskHandlerB = new TestTasks.CountingHandler<>();

    oneTimeTaskC = TestTasks.oneTime("OneTimeC", Integer.class, onetimeTaskHandlerC);
    onetimeTaskHandlerC = new TestTasks.CountingHandler<>();

    scheduleAnother =
        new ScheduleAnotherTaskHandler<>(
            oneTimeTaskA.instance("secondTask"), settableClock.now().plusSeconds(1));
    scheduleAnotherTask = TestTasks.oneTime("ScheduleAnotherTask", Void.class, scheduleAnother);

    savingHandler = new SavingHandler<>();
    savingTask = TestTasks.oneTime("SavingTask", String.class, savingHandler);

    scheduler =
        TestHelper.createManualScheduler(
                DB.getDataSource(), oneTimeTaskA, oneTimeTaskB, scheduleAnotherTask, savingTask)
            .clock(settableClock)
            .start();
  }

  @Test
  public void client_should_be_able_to_schedule_executions() {
    SchedulerClient client = create(DB.getDataSource()).build();

    // test deprecated method
    client.schedule(oneTimeTaskA.instance("1"), settableClock.now());
    assertFalse(client.scheduleIfNotExists(oneTimeTaskA.instance("1"), settableClock.now()));

    scheduler.runAnyDueExecutions();
    assertThat(onetimeTaskHandlerA.timesExecuted.get(), CoreMatchers.is(1));

    // test new method
    client.scheduleIfNotExists(oneTimeTaskA.instance("1"), settableClock.now());
    assertFalse(client.scheduleIfNotExists(oneTimeTaskA.instance("1"), settableClock.now()));

    scheduler.runAnyDueExecutions();
    assertThat(onetimeTaskHandlerA.timesExecuted.get(), CoreMatchers.is(2));
  }

  @Test
  public void should_be_able_to_schedule_other_executions_from_an_executionhandler() {
    scheduler.schedule(scheduleAnotherTask.instance("1"), settableClock.now());
    scheduler.runAnyDueExecutions();
    assertThat(scheduleAnother.timesExecuted, CoreMatchers.is(1));
    assertThat(onetimeTaskHandlerA.timesExecuted.get(), CoreMatchers.is(0));

    scheduler.tick(ofSeconds(1));
    scheduler.runAnyDueExecutions();
    assertThat(onetimeTaskHandlerA.timesExecuted.get(), CoreMatchers.is(1));
  }

  @Test
  public void client_should_be_able_to_fetch_executions_for_task() {
    SchedulerClient client = create(DB.getDataSource(), oneTimeTaskA, oneTimeTaskB).build();
    client.schedule(oneTimeTaskA.instance("1"), settableClock.now());
    client.schedule(oneTimeTaskA.instance("2"), settableClock.now());
    client.schedule(oneTimeTaskB.instance("10"), settableClock.now());
    client.schedule(oneTimeTaskB.instance("11"), settableClock.now());
    client.schedule(oneTimeTaskB.instance("12"), settableClock.now());

    assertThat(countAllExecutions(client), is(5));
    assertThat(client.getScheduledExecutions().size(), is(5));
    assertThat(countExecutionsForTask(client, oneTimeTaskA.getName(), Void.class), is(2));
    assertThat(countExecutionsForTask(client, oneTimeTaskB.getName(), Void.class), is(3));
    assertThat(
        client.getScheduledExecutionsForTask(oneTimeTaskB.getName(), Void.class).size(), is(3));
  }

  @Test
  public void client_should_be_able_to_fetch_single_scheduled_execution() {
    SchedulerClient client = create(DB.getDataSource(), oneTimeTaskA).build();
    client.schedule(oneTimeTaskA.instance("1"), settableClock.now());

    assertThat(
        client.getScheduledExecution(TaskInstanceId.of(oneTimeTaskA.getName(), "1")),
        not(OptionalMatchers.empty()));
    assertThat(
        client.getScheduledExecution(TaskInstanceId.of(oneTimeTaskA.getName(), "2")),
        OptionalMatchers.empty());
    assertThat(
        client.getScheduledExecution(TaskInstanceId.of(oneTimeTaskB.getName(), "1")),
        OptionalMatchers.empty());
  }

  @Test
  public void client_should_be_able_to_reschedule_executions() {
    String data1 = "data1";
    String data2 = "data2";

    scheduler.schedule(savingTask.instance("1", data1), settableClock.now().plusSeconds(1));
    scheduler.reschedule(savingTask.instance("1"), settableClock.now());
    scheduler.runAnyDueExecutions();
    assertThat(savingHandler.savedData, CoreMatchers.is(data1));

    scheduler.schedule(savingTask.instance("2", "none"), settableClock.now().plusSeconds(1));
    scheduler.reschedule(savingTask.instance("2"), settableClock.now(), data2);
    scheduler.runAnyDueExecutions();
    assertThat(savingHandler.savedData, CoreMatchers.is(data2));

    scheduler.tick(ofSeconds(1));
    scheduler.runAnyDueExecutions();
    assertThat(savingHandler.savedData, CoreMatchers.is(data2));
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void raw_client_should_be_able_to_fetch_executions() {
    TaskInstance<Integer> instance = oneTimeTaskC.instance("1", 5);

    SchedulerClient clientWithTypes = create(DB.getDataSource(), oneTimeTaskC).build();
    clientWithTypes.schedule(instance, settableClock.now());

    SchedulerClient clientWithoutTypes = create(DB.getDataSource()).build();

    Optional<ScheduledExecution<Object>> e1 = clientWithoutTypes.getScheduledExecution(instance);
    assertThat(e1, not(OptionalMatchers.empty()));
    assertRawData(e1.get(), 5);

    List<ScheduledExecution<Object>> allScheduled = clientWithoutTypes.getScheduledExecutions();
    assertThat(allScheduled, hasSize(1));
    assertRawData(allScheduled.get(0), 5);

    List<ScheduledExecution<Object>> scheduledForTask =
        clientWithoutTypes.getScheduledExecutionsForTask(instance.getTaskName());
    assertThat(scheduledForTask, hasSize(1));
    assertRawData(scheduledForTask.get(0), 5);
  }

  private void assertRawData(ScheduledExecution<Object> se, Integer expectedValue) {
    assertTrue(se.hasRawData());
    assertEquals(expectedValue, new JavaSerializer().deserialize(Integer.class, se.getRawData()));
  }

  private int countAllExecutions(SchedulerClient client) {
    AtomicInteger counter = new AtomicInteger(0);
    client.fetchScheduledExecutions(
        (ScheduledExecution<Object> execution) -> {
          counter.incrementAndGet();
        });
    return counter.get();
  }

  private <T> int countExecutionsForTask(
      SchedulerClient client, String taskName, Class<T> dataClass) {
    AtomicInteger counter = new AtomicInteger(0);
    client.fetchScheduledExecutionsForTask(
        taskName,
        dataClass,
        (ScheduledExecution<T> execution) -> {
          counter.incrementAndGet();
        });
    return counter.get();
  }

  public static class ScheduleAnotherTaskHandler<T> implements VoidExecutionHandler<T> {
    private final TaskInstance<Void> secondTask;
    private final Instant instant;
    public int timesExecuted = 0;

    public ScheduleAnotherTaskHandler(TaskInstance<Void> secondTask, Instant instant) {
      this.secondTask = secondTask;
      this.instant = instant;
    }

    @Override
    public void execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
      executionContext.getSchedulerClient().schedule(secondTask, instant);
      this.timesExecuted++;
    }
  }
}
