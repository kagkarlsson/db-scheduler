package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.Mappers;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.function.Consumer;

import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;

public class EmbeddedPostgresqlExtension implements AfterEachCallback {

    private static EmbeddedPostgres embeddedPostgresql;
    private static DataSource dataSource;
    private final Consumer<DataSource> initializeSchema;
    private final Consumer<DataSource> cleanupAfter;

    public EmbeddedPostgresqlExtension() {
        this(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);
    }

    public EmbeddedPostgresqlExtension(Consumer<DataSource> initializeSchema, Consumer<DataSource> cleanupAfter) {
        this.initializeSchema = initializeSchema;
        this.cleanupAfter = cleanupAfter;
        try {
            synchronized (this) {

                if (embeddedPostgresql == null) {
                    embeddedPostgresql = initPostgres();

                    HikariConfig config = new HikariConfig();
                    config.setDataSource(embeddedPostgresql.getDatabase("test", "test"));

                    dataSource = new HikariDataSource(config);

                    initializeSchema.accept(dataSource);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    private EmbeddedPostgres initPostgres() throws IOException {
        final EmbeddedPostgres newEmbeddedPostgresql = EmbeddedPostgres.builder().start();

        final JdbcRunner postgresJdbc = new JdbcRunner(newEmbeddedPostgresql.getPostgresDatabase());

        final Boolean databaseExists = postgresJdbc.query("SELECT 1 FROM pg_database WHERE datname = 'test'", NOOP, Mappers.NON_EMPTY_RESULTSET);
        if (!databaseExists) {
            postgresJdbc.execute("CREATE DATABASE test", NOOP);
        }

        final Boolean userExists = postgresJdbc.query("SELECT 1 FROM pg_catalog.pg_user WHERE usename = 'test'", NOOP, Mappers.NON_EMPTY_RESULTSET);
        if (!userExists) {
            postgresJdbc.execute("CREATE ROLE test LOGIN PASSWORD ''", NOOP);
        }

        postgresJdbc.execute("CREATE SCHEMA IF NOT EXISTS AUTHORIZATION test ", NOOP);

        return newEmbeddedPostgresql;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        cleanupAfter.accept(getDataSource());

    }
}
