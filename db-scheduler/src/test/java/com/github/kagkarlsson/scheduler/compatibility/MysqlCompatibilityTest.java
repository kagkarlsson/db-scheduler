package com.github.kagkarlsson.scheduler.compatibility;

import java.util.Properties;

import javax.sql.DataSource;

import org.junit.Ignore;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;

@Ignore
public class MysqlCompatibilityTest extends CompatibilityTest {

	public static final String JDBC_URL = "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC";
	public static final String JDBC_USER = "dummy";
	public static final String JDBC_PASSWORD = "dummy";

	@Override
	public DataSource getDataSource() {
		final DriverDataSource datasource = new DriverDataSource(JDBC_URL, "com.mysql.cj.jdbc.Driver", new Properties(), JDBC_USER, JDBC_PASSWORD);
		
		final HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setDataSource(datasource);
		return new HikariDataSource(hikariConfig);
	}
}
