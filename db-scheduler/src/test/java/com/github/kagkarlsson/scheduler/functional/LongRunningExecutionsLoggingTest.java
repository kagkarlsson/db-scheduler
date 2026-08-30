package com.github.kagkarlsson.scheduler.functional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.TestTasks.PausingHandler;
import com.github.kagkarlsson.scheduler.helper.TestableListener;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;

public class LongRunningExecutionsLoggingTest {
  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  @Test
  public void test_long_running_thread_logging() throws InterruptedException {
    PausingHandler<Void> handler = new PausingHandler<>();
    ListAppender<ILoggingEvent> appender = startAndGetLogListAppender();

    OneTimeTask<Void> customTask = Tasks.oneTime("custom-a", Void.class).execute(handler);

    TestableListener.Condition ranExecuteDue = TestableListener.Conditions.ranExecuteDue(2);
    TestableListener listener = TestableListener.create().waitConditions(ranExecuteDue).build();

    Scheduler scheduler =
        Scheduler.create(postgres.getDataSource(), customTask)
            .pollingInterval(Duration.ofMillis(100))
            .schedulerName(new SchedulerName.Fixed("test"))
            .longRunningExecutionsLoggingThreshold(Duration.ofMillis(40))
            .heartbeatInterval(Duration.ofMillis(40))
            .addSchedulerListener(listener)
            .build();
    stopScheduler.register(scheduler);

    scheduler.schedule(customTask.instance("1"), Instant.now());
    scheduler.start();
    handler.waitForExecute.await();

    ranExecuteDue.waitFor();

    handler.waitInExecuteUntil.countDown();

    List<ILoggingEvent> logEvents = appender.list;

    checkLogEvent(logEvents, Level.DEBUG, "Logging 1 long-running executions being processed.");
    checkLogEvent(
        logEvents,
        Level.WARN,
        "Execution with TaskInstance: task=custom-a, id=1, priority=0 is long-running (execution time: ");
  }

  private static void checkLogEvent(List<ILoggingEvent> logEvents, Level level, String message) {
    assertDoesNotThrow(
        () ->
            logEvents.stream()
                .filter(event -> event.getLevel() == level)
                .filter(event -> event.getFormattedMessage().startsWith(message))
                .findFirst()
                .orElseThrow());
  }

  private static ListAppender<ILoggingEvent> startAndGetLogListAppender() {
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();

    Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Scheduler.class);
    logger.setLevel(ch.qos.logback.classic.Level.ALL);
    logger.addAppender(appender);

    return appender;
  }
}
