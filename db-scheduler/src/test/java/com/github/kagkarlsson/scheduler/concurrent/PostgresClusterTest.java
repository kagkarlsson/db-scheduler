package com.github.kagkarlsson.scheduler.concurrent;

import static com.github.kagkarlsson.scheduler.concurrent.ClusterTests.NUMBER_OF_THREADS;
import static com.github.kagkarlsson.scheduler.concurrent.ClusterTests.testConcurrencyForPollingStrategy;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.jdbc.PostgreSqlJdbcCustomization;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("compatibility-cluster")
public class PostgresClusterTest {

  private static final Logger DEBUG_LOG = LoggerFactory.getLogger(PostgresClusterTest.class);
  @RegisterExtension public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();
  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  //    Enable if test gets flaky!
  //    @RegisterExtension
  //    public ChangeLogLevelsExtension changeLogLevels = new ChangeLogLevelsExtension(
  //        new LogLevelOverride("com.github.kagkarlsson.scheduler.Scheduler", Level.DEBUG),
  //        new LogLevelOverride("com.github.kagkarlsson.scheduler.ExecutePicked", Level.DEBUG),
  //        new LogLevelOverride("com.github.kagkarlsson.scheduler.Executor", Level.DEBUG),
  //        new LogLevelOverride("com.github.kagkarlsson.scheduler.FetchCandidates", Level.DEBUG)
  //    );

  @Test
  public void test_concurrency_optimistic_locking() throws InterruptedException {
    DEBUG_LOG.info("Starting test_concurrency_optimistic_locking");
    testConcurrencyForPollingStrategy(
        DB.getDataSource(),
        (SchedulerBuilder b) -> b.pollUsingFetchAndLockOnExecute(0, NUMBER_OF_THREADS * 3),
        stopScheduler);
  }

  @Test
  public void test_concurrency_select_for_update() throws InterruptedException {
    DEBUG_LOG.info("Starting test_concurrency_select_for_update");
    testConcurrencyForPollingStrategy(
        DB.getDataSource(),
        (SchedulerBuilder b) -> {
          b.pollUsingLockAndFetch(((double) NUMBER_OF_THREADS) / 2, NUMBER_OF_THREADS);
          b.jdbcCustomization(new PostgreSqlJdbcCustomization(false));
        },
        stopScheduler);
  }

  @Test
  public void test_concurrency_select_for_update_generic() throws InterruptedException {
    DEBUG_LOG.info("Starting test_concurrency_select_for_update");
    testConcurrencyForPollingStrategy(
        DB.getDataSource(),
        (SchedulerBuilder b) -> {
          b.pollUsingLockAndFetch(((double) NUMBER_OF_THREADS) / 2, NUMBER_OF_THREADS);
          b.jdbcCustomization(new PostgreSqlJdbcCustomization(true));
        },
        stopScheduler);
  }

  @Test
  public void test_concurrency_recurring() throws InterruptedException {
    Assertions.assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          ClusterTests.testRecurring(stopScheduler, DB.getDataSource());
        });
  }
}
