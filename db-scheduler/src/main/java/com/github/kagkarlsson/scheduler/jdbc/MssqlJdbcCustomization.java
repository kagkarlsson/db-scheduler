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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;

public class MssqlJdbcCustomization extends DefaultJdbcCustomization {

  @Override
  public String getName() {
    return "MSSQL";
  }

  @Override
  public void setInstant(PreparedStatement p, int index, Instant value) throws SQLException {
    p.setTimestamp(
        index,
        value != null ? Timestamp.from(value) : null,
        Calendar.getInstance(TimeZone.getTimeZone("UTC")));
  }
}
