/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples.helpers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * File-based HSQLDB so the demo can be stopped and restarted to observe state surviving across JVM
 * lifetimes. Files land under {@code ./target/} so {@code mvn clean} wipes them.
 */
public class PersistentHsqlDatasource {

  public static DataSource initDatabase(String name) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(
        "jdbc:hsqldb:file:./target/" + name + ";shutdown=true;hsqldb.write_delay=false");
    config.setUsername("sa");
    config.setPassword("");
    // single connection keeps the file-based DB's locking model simple for the demo
    config.setMaximumPoolSize(2);

    HikariDataSource dataSource = new HikariDataSource(config);
    createTableIfMissing(dataSource);
    return dataSource;
  }

  private static void createTableIfMissing(DataSource dataSource) {
    try (Connection c = dataSource.getConnection();
        Statement statement = c.createStatement()) {
      String createTables = readFile("/hsql_tables.sql");
      // The shipped DDL uses CREATE TABLE; wrap it so the demo is restart-safe.
      statement.execute(
          createTables.replaceFirst("(?i)create table", "create table if not exists"));
    } catch (SQLException | IOException e) {
      throw new RuntimeException("Failed to create tables", e);
    }
  }

  private static String readFile(String resource) throws IOException {
    try (BufferedReader in =
        new BufferedReader(
            new InputStreamReader(PersistentHsqlDatasource.class.getResourceAsStream(resource)))) {
      String line;
      StringBuilder createTables = new StringBuilder();
      while ((line = in.readLine()) != null) {
        createTables.append(line).append('\n');
      }
      return createTables.toString();
    }
  }
}
