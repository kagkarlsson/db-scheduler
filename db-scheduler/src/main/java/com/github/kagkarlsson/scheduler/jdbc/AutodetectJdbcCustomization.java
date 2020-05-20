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
package com.github.kagkarlsson.scheduler.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

public class AutodetectJdbcCustomization implements JdbcCustomization {

    private static final Logger LOG = LoggerFactory.getLogger(AutodetectJdbcCustomization.class);
    public static final String MICROSOFT_SQL_SERVER = "Microsoft SQL Server";
    private final JdbcCustomization jdbcCustomization;

    public AutodetectJdbcCustomization(DataSource dataSource) {
        JdbcCustomization detectedCustomization = new DefaultJdbcCustomization();

        LOG.debug("Detecting database...");
        try (Connection c = dataSource.getConnection()) {
            String databaseProductName = c.getMetaData().getDatabaseProductName();
            LOG.info("Detected database {}.", databaseProductName);

            if (databaseProductName.equals(MICROSOFT_SQL_SERVER)) {
                LOG.info("Using MSSQL jdbc-overrides.");
                detectedCustomization = new MssqlJdbcCustomization();
            }

        } catch (SQLException e) {
            LOG.error("Failed to detect database via getDatabaseMetadata. Using default.");
        }

        this.jdbcCustomization = detectedCustomization;
    }

    @Override
    public void setInstant(PreparedStatement p, int index, Instant value) throws SQLException {
        jdbcCustomization.setInstant(p, index, value);
    }

    @Override
    public Instant getInstant(ResultSet rs, String columnName) throws SQLException {
        return jdbcCustomization.getInstant(rs, columnName);
    }
}
