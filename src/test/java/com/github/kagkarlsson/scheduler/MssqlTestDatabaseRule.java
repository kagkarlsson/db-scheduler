package com.github.kagkarlsson.scheduler;

import com.google.common.io.CharStreams;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.hsqldb.Database;
import org.hsqldb.DatabaseManager;
import org.junit.rules.ExternalResource;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

public class MssqlTestDatabaseRule extends ExternalResource {

	private DataSource dataSource;

	@Override
	public void before() throws Throwable {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:sqlserver://SERVER;databaseName=DATABASE");
		config.setUsername("sa");
		config.setPassword("");

		dataSource = new HikariDataSource(config);

		doWithConnection(dataSource, c -> {

			try {
				Statement statement = c.createStatement();
				String createTables = CharStreams.toString(new InputStreamReader(getClass().getResourceAsStream("/mssql_tables.sql")));
				statement.execute(createTables);
				statement.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} catch (IOException e) {
				throw new RuntimeException("Failed to create tables", e);
			}
		});
	}

	@Override
	protected void after() {
		DatabaseManager.closeDatabases(Database.CLOSEMODE_IMMEDIATELY);
	}

	private void doWithConnection(DataSource dataSource, Consumer<Connection> consumer) {
		try (Connection connection = dataSource.getConnection()) {
			consumer.accept(connection);
		} catch (SQLException e) {
			throw new RuntimeException("Error getting connection from datasource.", e);
		}
	}


	public DataSource getDataSource() {
		return dataSource;
	}
}
