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
package com.github.kagkarlsson.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Mappers {

  public static final SingleResultMapper<Integer> SINGLE_INT =
      new SingleResultMapper<>(rs -> rs.getInt(1));

  public static final SingleResultMapper<Long> SINGLE_LONG =
      new SingleResultMapper<>(rs -> rs.getLong(1));

  public static final SingleResultMapper<String> SINGLE_STRING =
      new SingleResultMapper<>(rs -> rs.getString(1));

  public static final ResultSetMapper<Boolean> NON_EMPTY_RESULTSET = new NonEmptyResultMapper();

  public static class SingleResultMapper<T> implements ResultSetMapper<T> {

    private final RowMapper<T> rowMapper;

    public SingleResultMapper(RowMapper<T> rowMapper) {
      this.rowMapper = rowMapper;
    }

    @Override
    public T map(ResultSet rs) throws SQLException {
      boolean first = rs.next();
      if (!first) {
        throw new SingleResultExpected("Expected single result in resultset, but had none.");
      }
      final T result = rowMapper.map(rs);
      boolean second = rs.next();
      if (second) {
        throw new SingleResultExpected("Expected single result in resultset, but had more than 1.");
      }
      return result;
    }
  }

  public static class SingleResultExpected extends SQLRuntimeException {

    public SingleResultExpected(String message) {
      super(message);
    }
  }

  private static class NonEmptyResultMapper implements ResultSetMapper<Boolean> {
    @Override
    public Boolean map(ResultSet resultSet) throws SQLException {
      return resultSet.next();
    }
  }
}
