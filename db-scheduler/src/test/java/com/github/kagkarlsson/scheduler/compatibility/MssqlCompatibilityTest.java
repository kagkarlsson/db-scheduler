package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.Properties;

@SuppressWarnings("rawtypes")
@Tag("compatibility")
@Testcontainers
public class MssqlCompatibilityTest extends CompatibilityTest {

    @Container
    private static final MSSQLServerContainer MSSQL = new MSSQLServerContainer();
    private static HikariDataSource pooledDatasource;

    @BeforeAll
    private static void initSchema() {
        final DriverDataSource datasource = new DriverDataSource(MSSQL.getJdbcUrl(), "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            new Properties(), MSSQL.getUsername(), MSSQL.getPassword());

        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSource(datasource);
        pooledDatasource = new HikariDataSource(hikariConfig);

        // init schema
        DbUtils.runSqlResource("/mssql_tables.sql").accept(pooledDatasource);
    }
    @Override
    public DataSource getDataSource() {
        return pooledDatasource;
    }
}
