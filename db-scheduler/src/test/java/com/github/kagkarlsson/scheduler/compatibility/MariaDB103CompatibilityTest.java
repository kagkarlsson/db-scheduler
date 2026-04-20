package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import java.util.Optional;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("compatibility")
@Testcontainers
public class MariaDB103CompatibilityTest extends CompatibilityTest {

  @Container
  private static final MariaDBContainer MARIADB =
      new MariaDBContainer(DockerImageName.parse("mariadb").withTag("10.3"));

  private static HikariDataSource pooledDatasource;

  public MariaDB103CompatibilityTest() {
    super(false, false); // FIXLATER: fix syntax and enable
  }

  @BeforeAll
  static void initSchema() {
    final DriverDataSource datasource =
        new DriverDataSource(
            MARIADB.getJdbcUrl(),
            "org.mariadb.jdbc.Driver",
            new Properties(),
            MARIADB.getUsername(),
            MARIADB.getPassword());

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDataSource(datasource);
    // Force non-UTC session TZ so the round-trip test exercises "session TZ != JVM TZ".
    hikariConfig.setConnectionInitSql("SET time_zone = '-08:00'");
    pooledDatasource = new HikariDataSource(hikariConfig);

    // init schema
    DbUtils.runSqlResource("/mariadb_tables.sql").accept(pooledDatasource);
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
  public Optional<JdbcCustomization> getJdbcCustomization() {
    return Optional.of(new AutodetectJdbcCustomization(getDataSource(), true));
  }
}
