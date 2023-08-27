package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("compatibility")
@Testcontainers
public class Mysql5CompatibilityTest extends CompatibilityTest {

  @Container
  private static final MySQLContainer MY_SQL =
      new MySQLContainer(DockerImageName.parse("mysql").withTag("5.7.34"));

  private static HikariDataSource pooledDatasource;

  public Mysql5CompatibilityTest() {
    super(false);
  }

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

  @Override
  public DataSource getDataSource() {
    return pooledDatasource;
  }

  @Override
  public boolean commitWhenAutocommitDisabled() {
    return false;
  }
}
