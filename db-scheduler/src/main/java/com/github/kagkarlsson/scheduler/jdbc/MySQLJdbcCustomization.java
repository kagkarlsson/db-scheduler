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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLJdbcCustomization extends DefaultJdbcCustomization {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLJdbcCustomization.class);

  public MySQLJdbcCustomization(boolean persistTimestampInUTC) {
    super(persistTimestampInUTC);
    if (!persistTimestampInUTC) {
      LOG.warn(
          "{} does not support persistent timezones. "
              + "It is recommended to store time in UTC to avoid issues with for example DST",
          getClass().getName());
    }
  }

  @Override
  public String getName() {
    return "MySQL < v8";
  }

  @Override
  public String getQueryLimitPart(int limit) {
    return Queries.postgresSqlLimitPart(limit);
  }
}
