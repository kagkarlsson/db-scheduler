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

import java.util.Optional;

public class Queries {

  public static String selectForUpdate(
      String tableName,
      String orderPart,
      String limitPart,
      String requiredAndCondition,
      String postgresOracleStyleForUpdate,
      String sqlServerStyleForUpdate) {
    return "SELECT * FROM "
        + tableName
        + Optional.ofNullable(sqlServerStyleForUpdate).orElse("")
        + " WHERE picked = ? AND execution_time <= ? "
        + requiredAndCondition
        + orderPart
        + Optional.ofNullable(postgresOracleStyleForUpdate).orElse("")
        + limitPart;
  }

  public static String postgresSqlLimitPart(int limit) {
    return " LIMIT " + limit;
  }

  public static String ansiSqlLimitPart(int limit) {
    return " OFFSET 0 ROWS FETCH FIRST " + limit + " ROWS ONLY ";
  }

  public static String ansiSqlOrderPart(boolean orderByPriority) {
    return orderByPriority
        ? " ORDER BY priority DESC, execution_time ASC "
        : " ORDER BY execution_time ASC ";
  }
}
