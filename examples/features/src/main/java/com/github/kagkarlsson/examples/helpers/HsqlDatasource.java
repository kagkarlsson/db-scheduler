/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples.helpers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

public class HsqlDatasource {

    public static DataSource initDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:schedule_testing");
        config.setUsername("sa");
        config.setPassword("");

        HikariDataSource dataSource  = new HikariDataSource(config);

        doWithConnection(dataSource, c -> {

            try (Statement statement = c.createStatement()) {
                String createTables = readFile("/hsql_tables.sql");
                statement.execute(createTables);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create tables", e);
            }
        });

        return dataSource;
    }

    private static String readFile(String resource) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(HsqlDatasource.class.getResourceAsStream(resource)))) {
            String line;
            StringBuilder createTables = new StringBuilder();
            while ((line = in.readLine()) != null) {
                createTables.append(line);
            }
            return createTables.toString();
        }
    }

    private static void doWithConnection(DataSource dataSource, Consumer<Connection> consumer) {
        try (Connection connection = dataSource.getConnection()) {
            consumer.accept(connection);
        } catch (SQLException e) {
            throw new RuntimeException("Error getting connection from datasource.", e);
        }
    }
}
