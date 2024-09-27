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
package com.github.kagkarlsson.scheduler.jdbc;

import com.github.kagkarlsson.scheduler.task.Execution;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutodetectJdbcCustomization implements JdbcCustomization {

  private static final Logger LOG = LoggerFactory.getLogger(AutodetectJdbcCustomization.class);
  private static final Logger SILENCABLE_LOG =
      LoggerFactory.getLogger(LOG.getName() + ".utc_warning");
  public static final String MICROSOFT_SQL_SERVER = "Microsoft SQL Server";
  public static final String POSTGRESQL = "PostgreSQL";
  public static final String ORACLE = "Oracle";
  public static final String MYSQL = "MySQL";
  public static final String MARIADB = "MariaDB";
  private final JdbcCustomization jdbcCustomization;

  public AutodetectJdbcCustomization(DataSource dataSource) {
    this(dataSource, false);
  }

  public AutodetectJdbcCustomization(DataSource dataSource, boolean persistTimestampInUTC) {
    JdbcCustomization detectedCustomization = new DefaultJdbcCustomization(persistTimestampInUTC);

    LOG.debug("Detecting database...");
    try (Connection c = dataSource.getConnection()) {
      String databaseProductName = c.getMetaData().getDatabaseProductName();
      LOG.info("Detected database {}.", databaseProductName);

      if (databaseProductName.equals(MICROSOFT_SQL_SERVER)) {
        LOG.info("Using MSSQL jdbc-overrides.");
        if (persistTimestampInUTC) {
          LOG.info("Redundant 'persistTimestampInUTC' setting. MSSQL will always persist in UTC.");
        }
        detectedCustomization = new MssqlJdbcCustomization(true);
      } else if (databaseProductName.equals(POSTGRESQL)) {
        LOG.info("Using PostgreSQL jdbc-overrides.");
        detectedCustomization = new PostgreSqlJdbcCustomization(false, persistTimestampInUTC);
      } else if (databaseProductName.contains(ORACLE)) {
        LOG.info("Using Oracle jdbc-overrides.");
        detectedCustomization = new OracleJdbcCustomization(persistTimestampInUTC);
      } else if (databaseProductName.contains(MARIADB)) {
        LOG.info("Using MariaDB jdbc-overrides.");
        logWarningIfNotUTC("MariaDB", persistTimestampInUTC);
        detectedCustomization = new MariaDBJdbcCustomization(persistTimestampInUTC);
      } else if (databaseProductName.contains(MYSQL)) {
        int databaseMajorVersion = c.getMetaData().getDatabaseMajorVersion();
        String dbVersion = c.getMetaData().getDatabaseProductVersion();
        logWarningIfNotUTC("MySQL", persistTimestampInUTC);
        if (databaseMajorVersion >= 8) {
          LOG.info("Using MySQL jdbc-overrides version 8 and later. (v {})", dbVersion);
          detectedCustomization = new MySQL8JdbcCustomization(persistTimestampInUTC);
        } else {
          LOG.info("Using MySQL jdbc-overrides for version older than 8. (v {})", dbVersion);
          detectedCustomization = new MySQLJdbcCustomization(persistTimestampInUTC);
        }
      } else {
        if (persistTimestampInUTC) {
          LOG.info(
              "No database-specific jdbc-overrides applied. Behavior overridden to always store "
                  + "timestamps in zone UTC");
        } else {
          SILENCABLE_LOG.warn(
              "No database-specific jdbc-overrides applied. Assuming time-related columns "
                  + "to be of type compatibe with 'TIMESTAMP WITH TIME ZONE', i.e. zone is persisted."
                  + " If not, consider overriding to always UTC via '.alwaysPersistTimestampInUTC()'.");
        }
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
  public boolean supportsSingleStatementLockAndFetch() {
    return jdbcCustomization.supportsSingleStatementLockAndFetch();
  }

  @Override
  public List<Execution> lockAndFetchSingleStatement(
      JdbcTaskRepositoryContext ctx, Instant now, int limit, boolean orderByPriority) {
    return jdbcCustomization.lockAndFetchSingleStatement(ctx, now, limit, orderByPriority);
  }

  @Override
  public boolean supportsGenericLockAndFetch() {
    return jdbcCustomization.supportsGenericLockAndFetch();
  }

  @Override
  public String createGenericSelectForUpdateQuery(
      String tableName, int limit, String requiredAndCondition, boolean orderByPriority) {
    return jdbcCustomization.createGenericSelectForUpdateQuery(
        tableName, limit, requiredAndCondition, orderByPriority);
  }

  @Override
  public String createSelectDueQuery(
      String tableName, int limit, String andCondition, boolean orderByPriority) {
    return jdbcCustomization.createSelectDueQuery(tableName, limit, andCondition, orderByPriority);
  }

  @Override
  public String getName() {
    return jdbcCustomization.getName();
  }

  private void logWarningIfNotUTC(String database, boolean persistTimestampInUTC) {
    if (!persistTimestampInUTC) {
      SILENCABLE_LOG.warn(
          "{}-schema does not support persistent timezones. "
              + "It is recommended to store time in UTC to avoid issues with for example DST. "
              + "For first time users, use setting 'alwaysPersistTimestampInUtc' to achieve this. "
              + "Users upgrading from a version prior to v14.0.0 can either silence this logger, "
              + "or perform a controlled upgrade to UTC timestamps. All old instances "
              + "of the scheduler must be stopped and timestamps migrated to UTC before starting "
              + "again, using 'alwaysPersistTimestampInUtc=true'.",
          database);
    }
  }
}
