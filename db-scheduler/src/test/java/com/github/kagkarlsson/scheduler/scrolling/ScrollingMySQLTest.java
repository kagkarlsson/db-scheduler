package com.github.kagkarlsson.scheduler.scrolling;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * scrolling integration tests using MySQL database. These tests verify scroll pagination behavior
 * with a real MySQL database engine.
 */
@Testcontainers
public class ScrollingMySQLTest extends ScrollingTestBase {

  @Container
  private static final MySQLContainer MY_SQL =
      new MySQLContainer(DockerImageName.parse("mysql").withTag("8.3"));

  private static HikariDataSource pooledDatasource;

  @BeforeAll
  static void initSchema() {
    final DriverDataSource datasource =
        new DriverDataSource(
            MY_SQL.getJdbcUrl(),
            "com.mysql.cj.jdbc.Driver",
            new Properties(),
            MY_SQL.getUsername(),
            MY_SQL.getPassword());

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDataSource(datasource);
    pooledDatasource = new HikariDataSource(hikariConfig);

    // init schema
    DbUtils.runSqlResource("/mysql_tables.sql").accept(pooledDatasource);
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
}
