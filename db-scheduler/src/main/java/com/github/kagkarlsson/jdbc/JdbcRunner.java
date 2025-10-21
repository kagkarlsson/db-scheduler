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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcRunner {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcRunner.class);
  private final ConnectionSupplier connectionSupplier;
  private final TransactionContextProvider transactionContextProvider;

  public JdbcRunner(DataSource dataSource) {
    this(dataSource, false);
  }

  public JdbcRunner(DataSource dataSource, boolean commitWhenAutocommitDisabled) {
    this(
        new DataSourceConnectionSupplier(dataSource, commitWhenAutocommitDisabled),
        new ThreadLocalTransactionContextProvider());
  }

  public JdbcRunner(
      DataSource dataSource,
      boolean commitWhenAutocommitDisabled,
      TransactionContextProvider transactionContextProvider) {
    this(
        new DataSourceConnectionSupplier(dataSource, commitWhenAutocommitDisabled),
        transactionContextProvider);
  }

  public JdbcRunner(
      ConnectionSupplier connectionSupplier,
      TransactionContextProvider transactionContextProvider) {
    this.connectionSupplier = connectionSupplier;
    this.transactionContextProvider = transactionContextProvider;
  }

  /**
   * Creates a transactional JdbcRunner that can be used to execute operations in a single
   * transaction. Will currently not detect externally managed transactions (e.g.
   * Spring-transactions), only prevent nested transactions using <code>inTransaction(..)</code>.
   * Will always commit or rollback.
   *
   * @param doInTransaction
   * @return
   * @param <T>
   */
  public <T> T inTransaction(Function<JdbcRunner, T> doInTransaction) {
    return new TransactionManager(connectionSupplier, transactionContextProvider)
        .inTransaction(
            c -> {
              final JdbcRunner jdbc =
                  new JdbcRunner(new ExternallyManagedConnection(c), transactionContextProvider);
              return doInTransaction.apply(jdbc);
            });
  }

  public int execute(String query, PreparedStatementSetter setParameters) {
    return execute(
        query,
        setParameters,
        PreparedStatementExecutor.EXECUTE,
        new AfterExecution.ReturnStatementUpdateCount<>());
  }

  public <T> List<T> query(
      String query, PreparedStatementSetter setParameters, RowMapper<T> rowMapper) {
    return execute(
        query,
        setParameters,
        PreparedStatementExecutor.EXECUTE,
        (p, executeResult) -> mapResultSet(p, rowMapper));
  }

  public <T> T query(
      String query, PreparedStatementSetter setParameters, ResultSetMapper<T> resultSetMapper) {
    return execute(
        query,
        setParameters,
        PreparedStatementExecutor.EXECUTE,
        (p, executeResult) -> mapResultSet(p, resultSetMapper));
  }

  public <T> T execute(
      String query,
      PreparedStatementSetter setParameters,
      AfterExecution<T, Boolean> afterExecution) {
    return execute(query, setParameters, PreparedStatementExecutor.EXECUTE, afterExecution);
  }

  public <U> int[] executeBatch(
      String query, List<U> batchValues, BatchPreparedStatementSetter<U> setParameters) {

    PreparedStatementSetter setAllBatches =
        preparedStatement -> {
          for (U batchValue : batchValues) {
            setParameters.setParametersForRow(batchValue, preparedStatement);
            preparedStatement.addBatch();
          }
        };

    return execute(
        query,
        setAllBatches,
        PreparedStatementExecutor.EXECUTE_BATCH,
        (executedPreparedStatement, executeResult) -> executeResult);
  }

  private <T, U> T execute(
      String query,
      PreparedStatementSetter setParameters,
      PreparedStatementExecutor<U> executePreparedStatement,
      AfterExecution<T, U> afterExecution) {
    return withConnection(
        c -> {
          PreparedStatement preparedStatement = null;
          try {

            if (LOG.isDebugEnabled()) {
              LOG.debug("Executing SQL query [{}]", query);
            }

            try {
              preparedStatement = c.prepareStatement(query);
            } catch (SQLException e) {
              throw new SQLRuntimeException("Error when preparing statement.", e);
            }

            try {
              LOG.trace("Setting parameters of prepared statement.");
              setParameters.setParameters(preparedStatement);
            } catch (SQLException e) {
              throw new SQLRuntimeException(e);
            }

            try {
              LOG.trace("Executing prepared statement");
              U executeResult = executePreparedStatement.execute(preparedStatement);
              return afterExecution.doAfterExecution(preparedStatement, executeResult);
            } catch (SQLException e) {
              throw translateException(e);
            }

          } finally {
            nonThrowingClose(preparedStatement);
          }
        });
  }

  private void commitIfNecessary(Connection c) {
    try {
      if (shouldManageTransaction(c)) {
        c.commit();
      }
    } catch (SQLException e) {
      throw new SQLRuntimeException("Failed to commit.", e);
    }
  }

  private RuntimeException rollbackIfNecessary(Connection c, RuntimeException originalException) {
    try {
      if (shouldManageTransaction(c)) {
        c.rollback();
      }
      return originalException;
    } catch (SQLException e) {
      SQLRuntimeException rollbackException = new SQLRuntimeException("Failed to rollback.", e);
      rollbackException.addSuppressed(originalException);
      return rollbackException;
    }
  }

  private boolean shouldManageTransaction(Connection c) throws SQLException {
    if (connectionSupplier.isExternallyManagedConnection()) {
      // Do not commit/rollback since connection (and therefore transaction-lifecycle),
      // is managed externally
      return false;
    }

    if (c.getAutoCommit()) {
      // AUTO-COMMIT=true
      // Do not commit/rollback when auto-commit is enabled.
      // Statements are auto-committed after they are run.
      return false;
    } else {
      // AUTO-COMMIT=false
      if (!connectionSupplier.commitWhenAutocommitDisabled()) {
        // When auto-commit is disabled, we assume the transaction is externally managed
        // unless otherwise specified
        return false;
      } else {
        // Commit/rollback when auto-commit is disabled but the user has specified that
        // they always want commit/rollback, even though their DataSource is giving out
        // connections where auto-commit=false.
        // This has been requested by users, but the use-case is not very clear
        return true;
      }
    }
  }

  private SQLRuntimeException translateException(SQLException ex) {
    if (ex instanceof SQLIntegrityConstraintViolationException) {
      return new IntegrityConstraintViolation(ex);
    } else {
      return new SQLRuntimeException(ex);
    }
  }

  private <T> T withConnection(Function<Connection, T> doWithConnection) {
    Connection c;
    try {
      LOG.trace("Getting connection from datasource");
      c = connectionSupplier.getConnection();
    } catch (SQLException e) {
      throw new SQLRuntimeException("Unable to open connection", e);
    }

    try {
      final T result = doWithConnection.apply(c);
      commitIfNecessary(c);
      return result;
    } catch (RuntimeException e) {
      throw rollbackIfNecessary(c, e);
    } finally {
      // Do not close when connection is managed by TransactionManager
      if (!connectionSupplier.isExternallyManagedConnection()) {
        nonThrowingClose(c);
      }
    }
  }

  private <T> List<T> mapResultSet(
      PreparedStatement executedPreparedStatement, RowMapper<T> rowMapper) {
    return withResultSet(
        executedPreparedStatement,
        (ResultSet rs) -> {
          List<T> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rowMapper.map(rs));
          }
          return results;
        });
  }

  private <T> T mapResultSet(
      PreparedStatement executedPreparedStatement, ResultSetMapper<T> resultSetMapper) {
    return withResultSet(executedPreparedStatement, (ResultSet rs) -> resultSetMapper.map(rs));
  }

  private <T> T withResultSet(
      PreparedStatement executedPreparedStatement, DoWithResultSet<T> doWithResultSet) {
    ResultSet rs = null;
    try {
      try {
        rs = executedPreparedStatement.getResultSet();
      } catch (SQLException e) {
        throw new SQLRuntimeException(e);
      }

      try {
        return doWithResultSet.withResultSet(rs);
      } catch (SQLException e) {
        throw new SQLRuntimeException(e);
      }

    } finally {
      nonThrowingClose(rs);
    }
  }

  private void nonThrowingClose(AutoCloseable toClose) {
    if (toClose == null) {
      return;
    }
    try {
      LOG.trace("Closing " + toClose.getClass().getSimpleName());
      toClose.close();
    } catch (Exception e) {
      LOG.warn("Exception on close of " + toClose.getClass().getSimpleName(), e);
    }
  }

  interface AfterExecution<T, U> {
    T doAfterExecution(PreparedStatement executedPreparedStatement, U executeResult)
        throws SQLException;

    class ReturnStatementUpdateCount<U> implements AfterExecution<Integer, U> {

      @Override
      public Integer doAfterExecution(PreparedStatement executedPreparedStatement, U executeResult)
          throws SQLException {
        return executedPreparedStatement.getUpdateCount();
      }
    }
  }

  interface DoWithResultSet<T> {
    T withResultSet(ResultSet rs) throws SQLException;
  }
}
