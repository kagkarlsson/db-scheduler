package com.github.kagkarlsson.jdbc;

import static com.github.kagkarlsson.jdbc.Mappers.SINGLE_INT;
import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class CommitWhenAutocommitDisabledTest {

  @RegisterExtension public HsqlExtension database = new HsqlExtension();

  private JdbcRunner autocommitDisabledButBehaviorOverridden;
  private JdbcRunner autocommitDisabled;

  @BeforeEach
  public void setUp() throws SQLException {
    autocommitDisabledButBehaviorOverridden =
        new JdbcRunner(new DisableAutoCommit(database.getDataSource()), true);
    autocommitDisabled = new JdbcRunner(new DisableAutoCommit(database.getDataSource()), false);
  }

  @Test
  public void test_overridden_autocommit_behavior() {
    autocommitDisabledButBehaviorOverridden.execute("create table table1 ( column1 INT);", NOOP);
    assertThat(
        autocommitDisabledButBehaviorOverridden.execute(
            "insert into table1(column1) values (?)", setInt(1)),
        is(1));
    assertThat(
        autocommitDisabledButBehaviorOverridden.query("select * from table1", NOOP, SINGLE_INT),
        is(1));

    autocommitDisabled.execute("update table1 set column1 = ?", setInt(5));
    // should not have been committed
    assertThat(
        autocommitDisabledButBehaviorOverridden.query("select * from table1", NOOP, SINGLE_INT),
        is(1));

    autocommitDisabledButBehaviorOverridden.execute("update table1 set column1 = ?", setInt(2));
    // updated
    assertThat(
        autocommitDisabledButBehaviorOverridden.query("select * from table1", NOOP, SINGLE_INT),
        is(2));

    autocommitDisabled.inTransaction(
        txJdbc -> {
          txJdbc.execute("update table1 set column1 = ?", setInt(10));
          return null;
        });
    // updated
    assertThat(autocommitDisabled.query("select * from table1", NOOP, SINGLE_INT), is(10));

    try {
      autocommitDisabled.inTransaction(
          txJdbc -> {
            txJdbc.execute("update table1 set column1 = ?", setInt(11));
            throw new RuntimeException();
          });
    } catch (Exception ignored) {
    }
    // not updated
    assertThat(autocommitDisabled.query("select * from table1", NOOP, SINGLE_INT), is(10));

    autocommitDisabledButBehaviorOverridden.inTransaction(
        txJdbc -> {
          txJdbc.execute("update table1 set column1 = ?", setInt(11));
          return null;
        });
    // updated
    assertThat(
        autocommitDisabledButBehaviorOverridden.query("select * from table1", NOOP, SINGLE_INT),
        is(11));

    try {
      autocommitDisabledButBehaviorOverridden.inTransaction(
          txJdbc -> {
            txJdbc.execute("update table1 set column1 = ?", setInt(12));
            throw new RuntimeException();
          });
    } catch (Exception ignored) {
    }
    // not updated
    assertThat(
        autocommitDisabledButBehaviorOverridden.query("select * from table1", NOOP, SINGLE_INT),
        is(11));
  }

  @Test
  public void test_transactions() {
    autocommitDisabledButBehaviorOverridden.execute("create table table1 ( column1 INT);", NOOP);
    assertThat(
        autocommitDisabledButBehaviorOverridden.execute(
            "insert into table1(column1) values (?)", setInt(1)),
        is(1));
    assertThat(
        autocommitDisabledButBehaviorOverridden.query("select * from table1", NOOP, SINGLE_INT),
        is(1));

    autocommitDisabled.inTransaction(
        txJdbc -> {
          txJdbc.execute("update table1 set column1 = ?", setInt(10));
          return null;
        });
    // updated
    assertThat(autocommitDisabled.query("select * from table1", NOOP, SINGLE_INT), is(10));

    try {
      autocommitDisabled.inTransaction(
          txJdbc -> {
            txJdbc.execute("update table1 set column1 = ?", setInt(11));
            throw new RuntimeException();
          });
    } catch (Exception ignored) {
    }
    // not updated
    assertThat(autocommitDisabled.query("select * from table1", NOOP, SINGLE_INT), is(10));

    autocommitDisabledButBehaviorOverridden.inTransaction(
        txJdbc -> {
          txJdbc.execute("update table1 set column1 = ?", setInt(12));
          return null;
        });
    // updated
    assertThat(
        autocommitDisabledButBehaviorOverridden.query("select * from table1", NOOP, SINGLE_INT),
        is(12));

    try {
      autocommitDisabledButBehaviorOverridden.inTransaction(
          txJdbc -> {
            txJdbc.execute("update table1 set column1 = ?", setInt(13));
            throw new RuntimeException();
          });
    } catch (Exception ignored) {
    }
    // not updated
    assertThat(
        autocommitDisabledButBehaviorOverridden.query("select * from table1", NOOP, SINGLE_INT),
        is(12));
  }

  private PreparedStatementSetter setInt(int value) {
    return ps -> ps.setInt(1, value);
  }

  private static class DisableAutoCommit implements DataSource {
    private final DataSource underlying;

    DisableAutoCommit(DataSource underlying) {
      this.underlying = underlying;
    }

    @Override
    public Connection getConnection() throws SQLException {
      Connection c = underlying.getConnection();
      c.setAutoCommit(false);
      return c;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return underlying.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      underlying.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      underlying.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return underlying.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return underlying.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return underlying.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return underlying.isWrapperFor(iface);
    }
  }
}
