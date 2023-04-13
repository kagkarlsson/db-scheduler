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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class DefaultJdbcCustomization implements JdbcCustomization {

    @Override
    public void setInstant(PreparedStatement p, int index, Instant value) throws SQLException {
        p.setTimestamp(index, value != null ? Timestamp.from(value) : null);
    }

    @Override
    public Instant getInstant(ResultSet rs, String columnName) throws SQLException {
        return Optional.ofNullable(rs.getTimestamp(columnName)).map(Timestamp::toInstant).orElse(null);
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
        return false;
    }

    @Override
    public String getQueryLimitPart(int limit) {
        return "";
    }

    @Override
    public boolean supportsLockAndFetch() {
        return false;
    }

    @Override
    public List<Execution> lockAndFetch(JdbcTaskRepositoryContext ctx, Instant now, int limit) {
        throw new UnsupportedOperationException("lockAndFetch not supported for " + this.getClass().getName());
    }

    @Override
    public String getName() {
        return "Default";
    }
}
