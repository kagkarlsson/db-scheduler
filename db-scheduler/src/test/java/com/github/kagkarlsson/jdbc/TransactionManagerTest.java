package com.github.kagkarlsson.jdbc;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionManagerTest {

  @Mock private DataSource dataSource;
  @Mock private Connection connection;
  private TransactionManager tm;
  private ThreadLocalTransactionContextProvider txp;

  @BeforeEach
  public void setUp() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getAutoCommit()).thenReturn(false);

    txp = new ThreadLocalTransactionContextProvider();
    tm = new TransactionManager(new DataSourceConnectionSupplier(dataSource, false), txp);
  }

  @Test
  public void should_commit_if_no_exceptions() throws SQLException {
    tm.inTransaction((DoInTransaction<Void>) c -> null);
    verify(connection).getAutoCommit();
    verify(connection).commit();
    verify(connection).close();
    verifyNoMoreInteractions(connection);

    assertThat(txp.getCurrent(), nullValue());
  }

  @Test
  public void should_rollback_if_exception() throws SQLException {
    try {
      tm.inTransaction(
          (DoInTransaction<Void>)
              c -> {
                throw new SQLRuntimeException();
              });
      fail("Should have thrown exception");
    } catch (SQLRuntimeException e) {
    }

    verify(connection).getAutoCommit();
    verify(connection).rollback();
    verify(connection).close();
    verifyNoMoreInteractions(connection);

    assertThat(txp.getCurrent(), nullValue());
  }

  @Test
  public void should_rollback_if_sql_exception_on_commit() throws SQLException {
    doThrow(new SQLException()).when(connection).commit();
    try {
      tm.inTransaction((DoInTransaction<Void>) c -> null);
    } catch (SQLRuntimeException e) {
    }

    verify(connection).getAutoCommit();
    verify(connection).commit();
    verify(connection).rollback();
    verify(connection).close();
    verifyNoMoreInteractions(connection);

    assertThat(txp.getCurrent(), nullValue());
  }

  @Test
  public void should_close_on_exception_on_commit_and_rollback() throws SQLException {
    doThrow(new SQLException()).when(connection).commit();
    doThrow(new SQLException()).when(connection).rollback();
    try {
      tm.inTransaction((DoInTransaction<Void>) c -> null);
    } catch (SQLRuntimeException e) {
    }

    verify(connection).getAutoCommit();
    verify(connection).commit();
    verify(connection).rollback();
    verify(connection).close();
    verifyNoMoreInteractions(connection);

    assertThat(txp.getCurrent(), nullValue());
  }

  @Test
  public void should_restore_autocommit_if_enabled_on_connection_open() throws SQLException {
    when(connection.getAutoCommit()).thenReturn(true);
    tm.inTransaction((DoInTransaction<Void>) c -> null);

    verify(connection).getAutoCommit();
    verify(connection).setAutoCommit(true);
    verify(connection).commit();
    verify(connection).setAutoCommit(false);
    verify(connection).close();
    verifyNoMoreInteractions(connection);

    assertThat(txp.getCurrent(), nullValue());
  }
}
