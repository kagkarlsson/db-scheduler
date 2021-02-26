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

import com.github.kagkarlsson.jdbc.PreparedStatementSetter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.stream.Collectors.joining;

class QueryBuilder {
    private final String tableName;
    private final List<AndCondition> andConditions = new ArrayList<>();
    private Optional<String> orderBy = empty();

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

    String getQuery() {
        StringBuilder s = new StringBuilder();
        s.append("select * from ").append(tableName);

        if (!andConditions.isEmpty()) {
            s.append(" where ");
            s.append(andConditions.stream().map(AndCondition::getQueryPart).collect(joining(" and ")));
        }

        orderBy.ifPresent(o -> s.append(" order by ").append(o));

        return s.toString();
    }

    PreparedStatementSetter getPreparedStatementSetter() {
        return p -> {
            int parameterIndex = 1;
            for (AndCondition andCondition : andConditions) {
                parameterIndex = andCondition.setParameters(p, parameterIndex);
            }
        };
    }
}
