package com.github.kagkarlsson.scheduler.scrolling;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * scrolling integration tests using Microsoft SQL Server database. These tests verify scroll
 * pagination behavior with a real SQL Server database engine.
 */
@Testcontainers
@SuppressWarnings("rawtypes")
public class ScrollingSqlServerIntegrationTest extends ScrollingTestBase {

  @Container
  private static final MSSQLServerContainer MSSQL =
      new MSSQLServerContainer<>(
              DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-latest"))
          .withEnv("TZ", "UTC"); // Force UTC timezone to avoid conversion issues

  private static HikariDataSource pooledDatasource;

  @BeforeAll
  static void initSchema() {
    // Configure JDBC URL with UTC timezone to prevent conversion issues
    String jdbcUrl = MSSQL.getJdbcUrl() + ";serverTimezone=UTC;useLegacyDatetimeCode=false";

    final DriverDataSource datasource =
        new DriverDataSource(
            jdbcUrl,
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

  @AfterAll
  static void cleanup() {
    if (pooledDatasource != null) {
      pooledDatasource.close();
    }
  }

  @Override
  public DataSource getDataSource() {
    return pooledDatasource;
  }

  @Override
  protected void performDatabaseSpecificSetup() {
    DbUtils.clearTables(getDataSource());
  }
}
