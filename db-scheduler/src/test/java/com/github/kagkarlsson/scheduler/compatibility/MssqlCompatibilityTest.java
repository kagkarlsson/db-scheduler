package com.github.kagkarlsson.scheduler.compatibility;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import org.junit.Ignore;

import javax.sql.DataSource;
import java.util.Properties;

@Ignore
public class MssqlCompatibilityTest extends CompatibilityTest {

	public static final String JDBC_URL = "dummy";
	public static final String JDBC_USER = "dummy";
	public static final String JDBC_PASSWORD = "dummy";

	@Override
	public DataSource getDataSource() {
		final DriverDataSource datasource = new DriverDataSource(JDBC_URL, "com.microsoft.sqlserver.jdbc.SQLServerDriver", new Properties(), JDBC_USER, JDBC_PASSWORD);
		final HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setDataSource(datasource);
		return new HikariDataSource(hikariConfig);
	}

}
