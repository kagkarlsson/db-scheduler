package com.github.kagkarlsson.scheduler.concurrent;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.StopSchedulerExtension;
import com.github.kagkarlsson.scheduler.jdbc.MssqlJdbcCustomization;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import java.time.Duration;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@Tag("compatibility-cluster")
public class MssqlClusterTest {
  public static final int NUMBER_OF_THREADS = 10;
  private static final Logger DEBUG_LOG = LoggerFactory.getLogger(MssqlClusterTest.class);
  @RegisterExtension public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

  //    Enable if test gets flaky!
  //    @RegisterExtension
  //    public ChangeLogLevelsExtension changeLogLevels = new ChangeLogLevelsExtension(
  //        new LogLevelOverride("com.github.kagkarlsson.scheduler.Scheduler", Level.DEBUG),
  //        new LogLevelOverride("com.github.kagkarlsson.scheduler.ExecutePicked", Level.DEBUG),
  //        new LogLevelOverride("com.github.kagkarlsson.scheduler.Executor", Level.DEBUG),
  //        new LogLevelOverride("com.github.kagkarlsson.scheduler.FetchCandidates", Level.DEBUG)
  //    );

  @Container private static final MSSQLServerContainer MSSQL = new MSSQLServerContainer();
  private static DataSource pooledDatasource;

  @BeforeAll
  static void initSchema() {
    DriverDataSource datasource =
        new DriverDataSource(
            MSSQL.getJdbcUrl(),
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            new Properties(),
            MSSQL.getUsername(),
            MSSQL.getPassword());

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDataSource(datasource);
    pooledDatasource = new HikariDataSource(hikariConfig);

    // init schema
    DbUtils.dropTables(pooledDatasource);
    DbUtils.runSqlResource("/mssql_tables.sql").accept(pooledDatasource);
  }

  @Test
  public void test_concurrency_optimistic_locking() throws InterruptedException {
    DEBUG_LOG.info("Starting test_concurrency_optimistic_locking");
    ClusterTests.testConcurrencyForPollingStrategy(
        pooledDatasource,
        (SchedulerBuilder b) -> {
          b.pollUsingFetchAndLockOnExecute(0, NUMBER_OF_THREADS * 3);
          b.jdbcCustomization(new MssqlJdbcCustomization());
        },
        stopScheduler);
  }

  @Test // select-for-update does not really work for sql server, there are too many deadlocks..
  @Disabled
  public void test_concurrency_select_for_update_generic() throws InterruptedException {
    DEBUG_LOG.info("Starting test_concurrency_select_for_update");
    ClusterTests.testConcurrencyForPollingStrategy(
        pooledDatasource,
        (SchedulerBuilder b) -> {
          b.pollUsingLockAndFetch(((double) NUMBER_OF_THREADS) / 2, NUMBER_OF_THREADS);
          b.jdbcCustomization(new MssqlJdbcCustomization());
        },
        stopScheduler);
  }

  @Test
  public void test_concurrency_recurring() throws InterruptedException {
    Assertions.assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          ClusterTests.testRecurring(stopScheduler, pooledDatasource);
        });
  }
}
