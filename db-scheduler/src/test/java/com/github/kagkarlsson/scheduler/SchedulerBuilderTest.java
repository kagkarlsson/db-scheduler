package com.github.kagkarlsson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.github.kagkarlsson.scheduler.jdbc.DefaultJdbcCustomization;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class SchedulerBuilderTest {

  private final Logger logger = (Logger) LoggerFactory.getLogger(SchedulerBuilder.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

  private Scheduler scheduler;

  {
    logger.setLevel(Level.ALL);
    appender.start();
  }

  @BeforeEach
  public void addAppender() {
    logger.addAppender(appender);
  }

  @AfterEach
  public void removeAppender() {
    logger.detachAppender(appender);

    if (scheduler != null) {
      scheduler.stop(Duration.ZERO, Duration.ZERO);
    }
  }

  @Test
  public void should_log_configured_threads_when_scheduler_creates_executor_service() {
    scheduler = builder().threads(4).build();

    assertThat(schedulerConfigurationLog()).contains("threads=4");
  }

  @Test
  public void should_log_externally_managed_threads_when_executor_service_is_supplied() {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    ExecutorService dueExecutor = Executors.newSingleThreadExecutor();
    ScheduledExecutorService housekeeperExecutor = Executors.newSingleThreadScheduledExecutor();

    scheduler =
        builder()
            .threads(4)
            .executorService(executorService)
            .dueExecutor(dueExecutor)
            .housekeeperExecutor(housekeeperExecutor)
            .build();

    assertThat(schedulerConfigurationLog()).doesNotContain("threads=");
  }

  private SchedulerBuilder builder() {
    return Scheduler.create(mock(DataSource.class), List.of())
        .jdbcCustomization(new DefaultJdbcCustomization(false));
  }

  private String schedulerConfigurationLog() {
    return appender.list.stream()
        .map(ILoggingEvent::getFormattedMessage)
        .filter(message -> message.startsWith("Creating scheduler with configuration:"))
        .findFirst()
        .orElseThrow();
  }
}
