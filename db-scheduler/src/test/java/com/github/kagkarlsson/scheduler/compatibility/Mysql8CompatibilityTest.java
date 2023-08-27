package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("compatibility")
@Testcontainers
@Disabled // FIXLATER: enable when SKIP LOCKED is fixed for mysql
public class Mysql8CompatibilityTest extends CompatibilityTest {

  @Container
  private static final MySQLContainer MY_SQL =
      new MySQLContainer(DockerImageName.parse("mysql").withTag("8"));

  private static HikariDataSource pooledDatasource;

  public Mysql8CompatibilityTest() {
    super(true);
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
