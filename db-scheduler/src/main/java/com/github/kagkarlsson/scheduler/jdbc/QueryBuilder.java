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

import static java.util.Optional.empty;
import static java.util.stream.Collectors.joining;

import com.github.kagkarlsson.jdbc.PreparedStatementSetter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class QueryBuilder {
  private final String tableName;
  private final List<AndCondition> andConditions = new ArrayList<>();
  private Optional<String> orderBy = empty();
  private Optional<Integer> limitValue = empty();

  QueryBuilder(String tableName) {
    this.tableName = tableName;
  }

  static QueryBuilder selectFromTable(String tableName) {
    return new QueryBuilder(tableName);
  }

  QueryBuilder andCondition(AndCondition andCondition) {
    andConditions.add(andCondition);
    return this;
  }

  QueryBuilder orderBy(String orderBy) {
    this.orderBy = Optional.of(orderBy);
    return this;
  }

  QueryBuilder limit(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("Limit must be non-negative, was: " + limit);
    }
    this.limitValue = Optional.of(limit);
    return this;
  }

  String getQuery(JdbcCustomization jdbcCustomization) {
    StringBuilder s = new StringBuilder();
    s.append("select * from ").append(tableName);

    if (!andConditions.isEmpty()) {
      s.append(" where ");
      s.append(andConditions.stream().map(AndCondition::getQueryPart).collect(joining(" and ")));
    }

    orderBy.ifPresent(o -> s.append(" order by ").append(o));

    if (limitValue.isPresent() && jdbcCustomization.supportsExplicitQueryLimitPart()) {
      s.append(jdbcCustomization.getQueryLimitPart(limitValue.get()));
    }

    return s.toString();
  }

  PreparedStatementSetter getPreparedStatementSetter(JdbcCustomization jdbcCustomization) {
    return p -> {
      int parameterIndex = 1;
      for (AndCondition andCondition : andConditions) {
        parameterIndex = andCondition.setParameters(p, parameterIndex);
      }
      if (limitValue.isPresent() && !jdbcCustomization.supportsExplicitQueryLimitPart()) {
        p.setMaxRows(limitValue.get());
      }
    };
  }
}
