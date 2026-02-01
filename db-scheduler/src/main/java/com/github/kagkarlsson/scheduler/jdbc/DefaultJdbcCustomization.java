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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

public class DefaultJdbcCustomization implements JdbcCustomization {

  public static final Calendar UTC = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
  private final boolean persistTimestampInUTC;

  public DefaultJdbcCustomization(boolean persistTimestampInUTC) {

    this.persistTimestampInUTC = persistTimestampInUTC;
  }

  @Override
  public void setInstant(PreparedStatement p, int index, Instant value) throws SQLException {
    if (value == null) {
      p.setTimestamp(index, null);
      return;
    }

    if (persistTimestampInUTC) {
      p.setTimestamp(index, Timestamp.from(value), UTC);
    } else {
      p.setTimestamp(index, Timestamp.from(value));
    }
  }

  @Override
  public Instant getInstant(ResultSet rs, String columnName) throws SQLException {
    if (persistTimestampInUTC) {
      return Optional.ofNullable(rs.getTimestamp(columnName, UTC))
          .map(Timestamp::toInstant)
          .orElse(null);
    } else {
      return Optional.ofNullable(rs.getTimestamp(columnName))
          .map(Timestamp::toInstant)
          .orElse(null);
    }
  }

  @Override
  public void setTaskData(PreparedStatement p, int index, byte[] value) throws SQLException {
    p.setObject(index, value);
  }

  @Override
  public byte[] getTaskData(ResultSet rs, String columnName) throws SQLException {
    return rs.getBytes(columnName);
  }

  @Override
  public boolean supportsExplicitQueryLimitPart() {
    return true;
  }

  @Override
  public String getQueryLimitPart(int limit) {
    return Queries.ansiSqlLimitPart(limit);
  }

  @Override
  public boolean supportsSingleStatementLockAndFetch() {
    return false;
  }

  @Override
  public List<Execution> lockAndFetchSingleStatement(
      JdbcTaskRepositoryContext ctx, Instant now, int limit, boolean orderByPriority) {
    throw new UnsupportedOperationException(
        "lockAndFetch not supported for " + this.getClass().getName());
  }

  @Override
  public boolean supportsGenericLockAndFetch() {
    return false;
  }

  @Override
  public String createGenericSelectForUpdateQuery(
      String tableName, int limit, String requiredAndCondition, boolean orderByPriority) {
    throw new UnsupportedOperationException(
        "method must be implemented when supporting generic lock-and-fetch");
  }

  @Override
  public String createSelectDueQuery(
      String tableName, int limit, String andCondition, boolean orderByPriority) {
    final String explicitLimit = supportsExplicitQueryLimitPart() ? getQueryLimitPart(limit) : "";
    return "select * from "
        + tableName
        + " where picked = ? and execution_time <= ? and (state is null or state = 'ACTIVE') "
        + andCondition
        + Queries.ansiSqlOrderPart(orderByPriority)
        + explicitLimit;
  }

  @Override
  public String getName() {
    return "Default";
  }
}
