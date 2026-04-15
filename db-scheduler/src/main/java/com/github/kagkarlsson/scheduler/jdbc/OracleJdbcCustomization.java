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

import static com.github.kagkarlsson.scheduler.jdbc.Queries.selectForUpdate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class OracleJdbcCustomization extends DefaultJdbcCustomization {

  public OracleJdbcCustomization(boolean persistTimestampInUTC) {
    super(persistTimestampInUTC);
  }

  @Override
  public String getName() {
    return "Oracle";
  }

  @Override
  public void setInstant(PreparedStatement p, int index, Instant value) throws SQLException {
    if (value == null) {
      p.setTimestamp(index, null);
      return;
    }

    if (persistTimestampInUTC) {
      // Plain TIMESTAMP column (zoneless). ojdbc rejects setObject(OffsetDateTime) here
      // with ORA-18716. UTC Calendar gives a stable UTC wall-clock on the wire.
      p.setTimestamp(index, Timestamp.from(value), UTC);
    } else {
      // TIMESTAMP WITH TIME ZONE column. setTimestamp(ts, cal) is buggy on ojdbc (binds
      // session TZ's offset instead of Calendar's), so use setObject(OffsetDateTime)
      // which reliably attaches the value's offset.
      p.setObject(index, value.atOffset(ZoneOffset.UTC));
    }
  }

  @Override
  public Instant getInstant(ResultSet rs, String columnName) throws SQLException {
    if (persistTimestampInUTC) {
      // Delegate to parent's UTC-Calendar read path.
      return super.getInstant(rs, columnName);
    }
    OffsetDateTime odt = rs.getObject(columnName, OffsetDateTime.class);
    return odt == null ? null : odt.toInstant();
  }

  @Override
  public boolean supportsGenericLockAndFetch() {
    // FIXLATER: fix syntax and enable
    return false;
  }

  @Override
  public String createGenericSelectForUpdateQuery(
      String tableName, int limit, String requiredAndCondition, boolean orderByPriority) {
    return selectForUpdate(
        tableName,
        Queries.ansiSqlOrderPart(orderByPriority),
        Queries.ansiSqlLimitPart(limit),
        requiredAndCondition,
        " FOR UPDATE SKIP LOCKED ",
        null,
        null);
  }
}
