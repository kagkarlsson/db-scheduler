package com.github.kagkarlsson.scheduler;

import com.google.common.io.CharStreams;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.hsqldb.Database;
import org.hsqldb.DatabaseManager;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

public class HsqlTestDatabaseExtension implements BeforeEachCallback, AfterEachCallback {

	private DataSource dataSource;
    @Override
    public void afterEach(ExtensionContext extensionContext) {
        DatabaseManager.closeDatabases(Database.CLOSEMODE_IMMEDIATELY);
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:schedule_testing");
        config.setUsername("sa");
        config.setPassword("");

        dataSource = new HikariDataSource(config);

        doWithConnection(dataSource, c -> {

            try {
                Statement statement = c.createStatement();
                String createTables = CharStreams.toString(new InputStreamReader(getClass().getResourceAsStream("/hsql_tables.sql")));
                statement.execute(createTables);
                statement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create tables", e);
            }
        });

    }

    public DataSource getDataSource() {
        return dataSource;
    }

    private void doWithConnection(DataSource dataSource, Consumer<Connection> consumer) {
        try (Connection connection = dataSource.getConnection()) {
            consumer.accept(connection);
        } catch (SQLException e) {
            throw new RuntimeException("Error getting connection from datasource.", e);
        }
    }
}
