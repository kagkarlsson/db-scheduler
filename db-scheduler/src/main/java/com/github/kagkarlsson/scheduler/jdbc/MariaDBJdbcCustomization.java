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

public class MariaDBJdbcCustomization extends DefaultJdbcCustomization {
  private static final Logger LOG = LoggerFactory.getLogger(MariaDBJdbcCustomization.class);

  public MariaDBJdbcCustomization(boolean persistTimestampInUTC) {
    super(persistTimestampInUTC);
  }

  @Override
  public String getName() {
    return "MariaDB";
  }

  @Override
  public String getQueryLimitPart(int limit) {
    return Queries.postgresSqlLimitPart(limit);
  }

  @Override
  public boolean supportsGenericLockAndFetch() {
    // FIXLATER: fix syntax and enable it for versions of MariaDB that supports it
    return false;
  }

  @Override
  public String createGenericSelectForUpdateQuery(
      String tableName, int limit, String requiredAndCondition) {
    return selectForUpdate(
        tableName,
        Queries.postgresSqlLimitPart(limit),
        requiredAndCondition,
        " FOR UPDATE SKIP LOCKED ",
        null);
  }
}
