package com.github.kagkarlsson.scheduler.compatibility;

import ch.qos.logback.classic.Level;
import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.helper.ChangeLogLevelsExtension;
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
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("compatibility")
@Testcontainers
public class Oracle11gCompatibilityTest extends CompatibilityTest {
  @Container private static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-xe");
  private static HikariDataSource pooledDatasource;

//        Enable if test gets flaky!
//  @RegisterExtension
//  public ChangeLogLevelsExtension changeLogLevels =
//      new ChangeLogLevelsExtension(
//          new ChangeLogLevelsExtension.LogLevelOverride(
//              "com.github.kagkarlsson.scheduler", Level.DEBUG));

  public Oracle11gCompatibilityTest() {
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
