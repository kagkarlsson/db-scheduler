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

import com.github.kagkarlsson.scheduler.task.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

public class AutodetectJdbcCustomization implements JdbcCustomization {

    private static final Logger LOG = LoggerFactory.getLogger(AutodetectJdbcCustomization.class);
    public static final String MICROSOFT_SQL_SERVER = "Microsoft SQL Server";
    public static final String POSTGRESQL = "PostgreSQL";
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
            } else if (databaseProductName.equals(POSTGRESQL)) {
                LOG.info("Using PostgreSQL jdbc-overrides.");
                detectedCustomization = new PostgreSqlJdbcCustomization();
            }

        } catch (SQLException e) {
            LOG.error("Failed to detect database via getDatabaseMetadata. Using default.", e);
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

    @Override
    public void setTaskData(PreparedStatement p, int index, byte[] value) throws SQLException {
        jdbcCustomization.setTaskData(p, index, value);
    }

    @Override
    public byte[] getTaskData(ResultSet rs, String columnName) throws SQLException {
        return jdbcCustomization.getTaskData(rs, columnName);
    }

    @Override
    public boolean supportsExplicitQueryLimitPart() {
        return jdbcCustomization.supportsExplicitQueryLimitPart();
    }

    @Override
    public String getQueryLimitPart(int limit) {
        return jdbcCustomization.getQueryLimitPart(limit);
    }

    @Override
    public boolean supportsLockAndFetch() {
        return jdbcCustomization.supportsLockAndFetch();
    }

    @Override
    public List<Execution> lockAndFetch(JdbcTaskRepositoryContext ctx, Instant now, int limit) {
        return jdbcCustomization.lockAndFetch(ctx, now, limit);
    }

    @Override
    public String getName() {
        return jdbcCustomization.getName();
    }
}
