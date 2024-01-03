package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepositoryContext;
import com.github.kagkarlsson.scheduler.task.Execution;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;

public class ZoneSpecificJdbcCustomization implements JdbcCustomization {
  private final JdbcCustomization delegate;
  private final Calendar zoneIfNonePresent;

  public ZoneSpecificJdbcCustomization(JdbcCustomization delegate, Calendar zoneIfNonePresent) {
    this.delegate = delegate;
    this.zoneIfNonePresent = zoneIfNonePresent;
  }

  @Override
  public String getName() {
    return delegate.getName() + "-zoned";
  }

  @Override
  public void setInstant(PreparedStatement p, int index, Instant value) throws SQLException {
    p.setTimestamp(index, value != null ? Timestamp.from(value) : null, zoneIfNonePresent);
  }

  @Override
  public Instant getInstant(ResultSet rs, String columnName) throws SQLException {
    return Optional.ofNullable(rs.getTimestamp(columnName, zoneIfNonePresent)).map(Timestamp::toInstant).orElse(null);
  }

  @Override
  public void setTaskData(PreparedStatement p, int index, byte[] value) throws SQLException {
    delegate.setTaskData(p, index, value);
  }

  @Override
  public byte[] getTaskData(ResultSet rs, String columnName) throws SQLException {
    return delegate.getTaskData(rs, columnName);
  }

  @Override
  public boolean supportsExplicitQueryLimitPart() {
    return delegate.supportsExplicitQueryLimitPart();
  }

  @Override
  public String getQueryLimitPart(int limit) {
    return delegate.getQueryLimitPart(limit);
  }

  @Override
  public boolean supportsSingleStatementLockAndFetch() {
    return delegate.supportsSingleStatementLockAndFetch();
  }

  @Override
  public List<Execution> lockAndFetchSingleStatement(JdbcTaskRepositoryContext ctx, Instant now,
    int limit) {
    return delegate.lockAndFetchSingleStatement(ctx, now, limit);
  }

  @Override
  public boolean supportsGenericLockAndFetch() {
    return delegate.supportsGenericLockAndFetch();
  }

  @Override
  public String createGenericSelectForUpdateQuery(String tableName, int limit,
    String requiredAndCondition) {
    return delegate.createGenericSelectForUpdateQuery(tableName, limit, requiredAndCondition);
  }

  @Override
  public String createSelectDueQuery(String tableName, int limit, String andCondition) {
    return delegate.createSelectDueQuery(tableName, limit, andCondition);
  }
}
