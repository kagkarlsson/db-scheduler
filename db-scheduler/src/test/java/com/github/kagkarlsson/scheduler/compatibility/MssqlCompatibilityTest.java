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
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * To test manually, disable testcontainers stuff and use Azure SQL Edge
 *
 * <p>docker run --cap-add SYS_PTRACE -e 'ACCEPT_EULA=1' -e 'MSSQL_SA_PASSWORD=bigSt%rongPwd' -p
 * 1433:1433 --name sqledge -d mcr.microsoft.com/azure-sql-edge
 *
 * <p>String jdbcUrl = "jdbc:sqlserver://localhost:1433"; DataSource datasource = new
 * DriverDataSource(jdbcUrl, "com.microsoft.sqlserver.jdbc.SQLServerDriver", new Properties(), "SA",
 * "bigSt%rongPwd");
 */
@SuppressWarnings("rawtypes")
@Tag("compatibility")
@Testcontainers
@Disabled
public class MssqlCompatibilityTest extends CompatibilityTest {

  @Container private static final MSSQLServerContainer MSSQL = new MSSQLServerContainer();
  private static DataSource pooledDatasource;

  public MssqlCompatibilityTest() {
    super(true, true);
  }

  @BeforeAll
  static void initSchema() {
    //      For MANUAL testing, see javadoc comment
    //        String jdbcUrl = "jdbc:sqlserver://localhost:1433";
    //        DataSource datasource =
    //            new DriverDataSource(
    //                jdbcUrl,
    //                "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    //                new Properties(),
    //                "SA",
    //                "bigSt%rongPwd");
    final DriverDataSource datasource =
        new DriverDataSource(
            MSSQL.getJdbcUrl(),
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            new Properties(),
            MSSQL.getUsername(),
            MSSQL.getPassword());

    //    for debugging SQL
    //            datasource = ProxyDataSourceBuilder.create(datasource)
    //                .logQueryBySlf4j()
    //                .build();

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDataSource(datasource);
    pooledDatasource = new HikariDataSource(hikariConfig);

    // init schema
    DbUtils.dropTables(pooledDatasource);
    DbUtils.runSqlResource("/mssql_tables.sql").accept(pooledDatasource);
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
