/**
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.jdbc;

import com.github.kagkarlsson.scheduler.task.Execution;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

public interface JdbcCustomization {

    String getName();

    void setInstant(PreparedStatement p, int index, Instant value) throws SQLException;

    Instant getInstant(ResultSet rs, String columnName) throws SQLException;

    void setTaskData(PreparedStatement p, int index, byte[] value) throws SQLException;

    byte[] getTaskData(ResultSet rs, String columnName) throws SQLException;

    boolean supportsExplicitQueryLimitPart();

    String getQueryLimitPart(int limit);

    boolean supportsLockAndFetch();

    List<Execution> lockAndFetch(JdbcTaskRepositoryContext ctx, Instant now, int limit);
}
