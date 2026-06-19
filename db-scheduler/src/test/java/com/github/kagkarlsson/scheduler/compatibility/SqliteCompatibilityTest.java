package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.sqlite.SQLiteDataSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("compatibility")
@Testcontainers
public class SqliteCompatibilityTest extends CompatibilityTest {
  private static HikariDataSource pooledDatasource;

  public SqliteCompatibilityTest() {
    super(false, true);
  }

  @BeforeAll
  static void initSchema() {
    final SQLiteDataSource datasource = new SQLiteDataSource();
    datasource.setUrl("jdbc:sqlite:memory:myDb");

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDataSource(datasource);
    hikariConfig.setMaximumPoolSize(1);
    pooledDatasource = new HikariDataSource(hikariConfig);

    DbUtils.dropTables(pooledDatasource);
    DbUtils.runSqlResource("/sqlite_tables.sql").accept(pooledDatasource);
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
}
