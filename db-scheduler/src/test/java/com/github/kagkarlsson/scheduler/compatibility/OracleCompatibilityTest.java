package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.OracleJdbcCustomization;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

@Tag("compatibility")
@Testcontainers
public class OracleCompatibilityTest extends CompatibilityTest {
  @Container
  private static final OracleContainer ORACLE =
      new OracleContainer("gvenzl/oracle-free:slim-faststart");

  private static HikariDataSource pooledDatasource;

  //        Enable if test gets flaky!
  //  @RegisterExtension
  //  public ChangeLogLevelsExtension changeLogLevels =
  //      new ChangeLogLevelsExtension(
  //          new ChangeLogLevelsExtension.LogLevelOverride(
  //              "com.github.kagkarlsson.scheduler", Level.DEBUG));

  public OracleCompatibilityTest() {
    super(false, true); // FIXLATER: fix syntax and enable
  }

  @BeforeAll
  static void initSchema() {
    final DriverDataSource datasource =
        new DriverDataSource(
            ORACLE.getJdbcUrl(),
            "oracle.jdbc.OracleDriver",
            new Properties(),
            ORACLE.getUsername(),
            ORACLE.getPassword());

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDataSource(datasource);
    hikariConfig.setMaximumPoolSize(10);
    pooledDatasource = new HikariDataSource(hikariConfig);

    // init schema
    DbUtils.runSqlResource("/oracle_tables.sql", true).accept(pooledDatasource);
  }

  @BeforeEach
  void overrideSchedulerShutdown() {
    stopScheduler.setWaitBeforeInterrupt(Duration.ofMillis(100));
  }

  @Override
  public DataSource getDataSource() {
    return pooledDatasource;
  }

  @Override
  public boolean commitWhenAutocommitDisabled() {
    return false;
  }

  @Override
  public Optional<JdbcCustomization> getJdbcCustomizationForUTCTimestampTest() {
    return Optional.of(new OracleJdbcCustomization(true));
  }
}
