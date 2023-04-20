package com.github.kagkarlsson.scheduler;

import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;
import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.Mappers;
import com.github.kagkarlsson.jdbc.PreparedStatementSetter;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.function.Consumer;
import javax.sql.DataSource;

public class DbUtils {

    public static void clearTables(DataSource dataSource) {
        new JdbcRunner(dataSource, true).execute("delete from " + DEFAULT_TABLE_NAME, NOOP);
    }

    public static Consumer<DataSource> runSqlResource(String resource) {
        return dataSource -> {
            final JdbcRunner jdbcRunner = new JdbcRunner(dataSource);
            try {
                final String statements =
                        CharStreams.toString(new InputStreamReader(DbUtils.class.getResourceAsStream(resource)));
                jdbcRunner.execute(statements, NOOP);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static int countExecutions(DataSource dataSource) {
        return new JdbcRunner(dataSource)
                .query("select count(*) from " + DEFAULT_TABLE_NAME, PreparedStatementSetter.NOOP, Mappers.SINGLE_INT);
    }
}
