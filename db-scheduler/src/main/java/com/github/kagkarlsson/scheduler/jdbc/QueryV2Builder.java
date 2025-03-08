package com.github.kagkarlsson.scheduler.jdbc;

import static java.util.stream.Collectors.joining;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.PreparedStatementSetter;
import com.github.kagkarlsson.jdbc.ResultSetMapper;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class QueryV2Builder {

  private final String tableName;
  private final JdbcRunner jdbcRunner;
  private final List<AndV2Condition> andConditions = new ArrayList<>();

  private QueryV2Builder(String tableName, JdbcRunner jdbcRunner) {
    this.tableName = tableName;
    this.jdbcRunner = jdbcRunner;
  }

  public static QueryV2Builder selectFromTable(String tableName, JdbcRunner jdbcRunner) {
    return new QueryV2Builder(tableName, jdbcRunner);
  }

  public QueryV2Builder andCondition(String columnAndOperator, Object value) {
    this.andConditions.add(new AndV2Condition(columnAndOperator, value));
    return this;
  }

  public <T> T execute(ResultSetMapper<T> resultSetMapper) {
    String query = generateQuery();
    PreparedStatementSetter setter = generatePreparedStatementSetter();

    return jdbcRunner.query(query, setter, resultSetMapper);
  }

  String generateQuery() {
    StringBuilder q = new StringBuilder();
    q.append("SELECT *").append(" FROM ").append(tableName);
    if (!andConditions.isEmpty()) {
      q.append(" WHERE ");
      q.append(
          andConditions.stream()
              .map(condition -> condition.columnAndOperator + "?")
              .collect(joining(" AND ")));
    }

    return q.toString();
  }

  PreparedStatementSetter generatePreparedStatementSetter() {
    return preparedStatement -> {
      int index = 1;
      for (AndV2Condition condition : andConditions) {
        setParameter(preparedStatement, index++, condition.value);
      }
    };
  }

  private void setParameter(PreparedStatement ps, int index, Object value) throws SQLException {
    if (value instanceof String) {
      ps.setString(index, (String)value);
    } else {
      throw new RuntimeException("Unsupported type: " + value.getClass());
    }
  }


  private static class AndV2Condition {
    private final String columnAndOperator;
    private final Object value;

    public AndV2Condition(String columnAndOperator, Object value) {
      this.columnAndOperator = columnAndOperator;
      this.value = value;
    }
  }
}
