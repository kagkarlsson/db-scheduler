package com.github.kagkarlsson.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.github.kagkarlsson.scheduler.jdbc.DefaultJdbcCustomization;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class SchedulerBuilderTest {

  private final Logger logger = (Logger) LoggerFactory.getLogger(SchedulerBuilder.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

  {
    logger.setLevel(ch.qos.logback.classic.Level.ALL);
    appender.start();
  }

  @BeforeEach
  public void addAppender() {
    logger.addAppender(appender);
  }

  @AfterEach
  public void removeAppender() {
    logger.detachAppender(appender);
  }

  @Test
  public void should_use_fixed_executor_service_thread_count_when_threads_not_configured() {
    ExecutorService executorService = Executors.newFixedThreadPool(3);

    try {
      Scheduler scheduler = schedulerBuilder().executorService(executorService).build();

      assertThat(scheduler.threadpoolSize, is(3));
      assertThat(appender.list.get(0).getFormattedMessage(), containsString("threads=3"));
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void should_use_configured_threads_over_fixed_executor_service_thread_count() {
    ExecutorService executorService = Executors.newFixedThreadPool(3);

    try {
      Scheduler scheduler = schedulerBuilder().executorService(executorService).threads(5).build();

      assertThat(scheduler.threadpoolSize, is(5));
      assertThat(appender.list.get(0).getFormattedMessage(), containsString("threads=5"));
    } finally {
      executorService.shutdownNow();
    }
  }

  private SchedulerBuilder schedulerBuilder() {
    return Scheduler.create(mock(DataSource.class))
        .jdbcCustomization(new DefaultJdbcCustomization(false))
        .schedulerName(new SchedulerName.Fixed("test"));
  }
}
