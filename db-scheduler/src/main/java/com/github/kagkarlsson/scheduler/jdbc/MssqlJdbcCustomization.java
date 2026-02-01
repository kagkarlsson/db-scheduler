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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MssqlJdbcCustomization extends DefaultJdbcCustomization {
  private static final Logger LOG = LoggerFactory.getLogger(MssqlJdbcCustomization.class);

  public MssqlJdbcCustomization() {
    super(true);
  }

  public MssqlJdbcCustomization(boolean persistTimestampInUTC) {
    super(persistTimestampInUTC);
    if (!persistTimestampInUTC) {
      LOG.warn(
          "{} must explicitly specify timezone when persisting a timestamp. "
              + "Persisting timestamp with undefined timezone is not recommended and will likely cause issues",
          getClass().getName());
    }
  }

  @Override
  public String getName() {
    return "MSSQL";
  }

  @Override
  public boolean supportsGenericLockAndFetch() {
    // Currently supported, but not recommended because of deadlock issues
    return true;
  }

  @Override
  public String createSelectDueQuery(
      String tableName, int limit, String andCondition, boolean orderByPriority) {
    return "SELECT "
        + " * FROM "
        + tableName
        // try reading past locked rows to see if that helps on deadlock-warnings
        + " WITH (READPAST) WHERE picked = ? AND execution_time <= ? AND (state is null OR state = 'ACTIVE') "
        + andCondition
        + Queries.ansiSqlOrderPart(orderByPriority)
        + getQueryLimitPart(limit);
  }

  @Override
  public String createGenericSelectForUpdateQuery(
      String tableName, int limit, String requiredAndCondition, boolean orderByPriority) {
    return selectForUpdate(
        tableName,
        Queries.ansiSqlOrderPart(orderByPriority),
        Queries.ansiSqlLimitPart(limit),
        requiredAndCondition,
        null,
        " WITH (READPAST,ROWLOCK) ",
        null);
  }
}
