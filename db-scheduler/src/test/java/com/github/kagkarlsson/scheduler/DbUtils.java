package com.github.kagkarlsson.scheduler;

import com.google.common.io.CharStreams;
import com.github.kagkarlsson.jdbc.JdbcRunner;

import javax.sql.DataSource;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.function.Consumer;

import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;
import static com.github.kagkarlsson.scheduler.JdbcTaskRepository.DEFAULT_TABLE_NAME;

public class DbUtils {

	public static void clearTables(DataSource dataSource) {
		new JdbcRunner(dataSource).execute("delete from " + DEFAULT_TABLE_NAME, NOOP);
	}

	public static Consumer<DataSource> runSqlResource(String resource) {
		return dataSource -> {

			final JdbcRunner jdbcRunner = new JdbcRunner(dataSource);
			try {
				final String statements = CharStreams.toString(new InputStreamReader(DbUtils.class.getResourceAsStream(resource)));
				jdbcRunner.execute(statements, NOOP);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		};
	}
}
