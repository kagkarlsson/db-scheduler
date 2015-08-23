package com.kagkarlsson.scheduler;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.kagkarlsson.jdbc.JdbcRunner;

import javax.sql.DataSource;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.function.Consumer;

import static com.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;

public class DbUtils {

	public static void clearTables(DataSource dataSource) {
		new JdbcRunner(dataSource).execute("delete from scheduled_tasks", NOOP);
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
