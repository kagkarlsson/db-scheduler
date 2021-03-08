package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.Properties;

@Tag("compatibility")
@Testcontainers
public class MysqlCompatibilityTest extends CompatibilityTest {

    @Container
    private static final MySQLContainer MY_SQL = new MySQLContainer();
    private static HikariDataSource pooledDatasource;

    public MysqlCompatibilityTest() { super(false);}

    @BeforeAll
    private static void initSchema() {
        final DriverDataSource datasource = new DriverDataSource(MY_SQL.getJdbcUrl(), "com.mysql.cj.jdbc.Driver", new Properties(), MY_SQL.getUsername(), MY_SQL.getPassword());

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
