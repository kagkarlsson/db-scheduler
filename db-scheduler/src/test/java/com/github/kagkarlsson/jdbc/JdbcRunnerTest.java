package com.github.kagkarlsson.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class JdbcRunnerTest {

  public static final String INSERT = "insert into table1(column1) values (?)";
  @RegisterExtension public HsqlExtension database = new HsqlExtension();
  private JdbcRunner jdbcRunner;

  @BeforeEach
  public void setUp() {
    jdbcRunner = new JdbcRunner(database.getDataSource(), false);
  }

  @Test
  public void test_basics() {
    jdbcRunner.execute("create table table1 ( column1 INT);", PreparedStatementSetter.NOOP);
    final int inserted =
        jdbcRunner.execute("insert into table1(column1) values (?)", ps -> ps.setInt(1, 1));
    assertThat(inserted, is(1));

    final List<Integer> rowMapped =
        jdbcRunner.query(
            "select * from table1", PreparedStatementSetter.NOOP, new TableRowMapper());
    assertThat(rowMapped, hasSize(1));
    assertThat(rowMapped.get(0), is(1));

    assertThat(
        jdbcRunner.query("select * from table1", PreparedStatementSetter.NOOP, Mappers.SINGLE_INT),
        is(1));

    final int updated =
        jdbcRunner.execute(
            "update table1 set column1 = ? where column1 = ?",
            ps -> {
              ps.setInt(1, 5);
              ps.setInt(2, 1);
            });
    assertThat(updated, is(1));
  }

  @Test
  public void test_map_multiple_rows() {
    jdbcRunner.execute("create table table1 ( column1 INT);", PreparedStatementSetter.NOOP);
    jdbcRunner.execute("insert into table1(column1) values (1)", PreparedStatementSetter.NOOP);
    jdbcRunner.execute("insert into table1(column1) values (2)", PreparedStatementSetter.NOOP);

    final List<Integer> rowMapped =
        jdbcRunner.query(
            "select * from table1", PreparedStatementSetter.NOOP, new TableRowMapper());
    assertThat(rowMapped, hasSize(2));
    assertThat(rowMapped.get(0), is(1));
    assertThat(rowMapped.get(1), is(2));

    final List<Integer> resultSetMapped =
        jdbcRunner.query(
            "select * from table1", PreparedStatementSetter.NOOP, new TableRowMapper());
    assertThat(resultSetMapped, hasSize(2));
    assertThat(resultSetMapped.get(0), is(1));
    assertThat(resultSetMapped.get(1), is(2));
  }

  @Test
  public void test_batch_insert() {
    jdbcRunner.execute("create table table1 ( column1 INT);", PreparedStatementSetter.NOOP);
    List<Integer> values = Arrays.asList(1, 2, 3);
    int[] updated =
        jdbcRunner.executeBatch(
            "insert into table1(column1) values (?)",
            values,
            (value, preparedStatement) -> preparedStatement.setInt(1, value));

    final List<Integer> rowMapped =
        jdbcRunner.query(
            "select * from table1 order by column1 asc",
            PreparedStatementSetter.NOOP,
            new TableRowMapper());
    assertThat(IntStream.of(updated).sum(), is(3));
    assertThat(rowMapped, hasSize(3));
    assertThat(rowMapped.get(0), is(1));
    assertThat(rowMapped.get(1), is(2));
    assertThat(rowMapped.get(2), is(3));
  }

  @Test
  public void should_map_constraint_violations_to_custom_exception_for_primary_key_constraint() {
    Assertions.assertThrows(
        IntegrityConstraintViolation.class,
        () -> {
          jdbcRunner.execute(
              "create table table1 ( column1 INT PRIMARY KEY);", PreparedStatementSetter.NOOP);
          jdbcRunner.execute(
              "insert into table1(column1) values (1)", PreparedStatementSetter.NOOP);
          jdbcRunner.execute(
              "insert into table1(column1) values (1)", PreparedStatementSetter.NOOP);
        });
  }

  @Test
  public void should_map_constraint_violations_to_custom_exception_for_unique_constraint_() {
    Assertions.assertThrows(
        IntegrityConstraintViolation.class,
        () -> {
          jdbcRunner.execute("create table table1 ( column1 INT);", PreparedStatementSetter.NOOP);
          jdbcRunner.execute(
              "alter table table1 add constraint col1_uidx unique (column1);",
              PreparedStatementSetter.NOOP);
          jdbcRunner.execute(
              "insert into table1(column1) values (1)", PreparedStatementSetter.NOOP);
          jdbcRunner.execute(
              "insert into table1(column1) values (1)", PreparedStatementSetter.NOOP);
        });
  }

  @Test
  public void test_in_transction() {
    jdbcRunner.execute("create table table1 ( column1 INT);", PreparedStatementSetter.NOOP);
    jdbcRunner.inTransaction(
        txRunner -> {
          txRunner.execute(INSERT, ps -> ps.setInt(1, 1));
          txRunner.execute(INSERT, ps -> ps.setInt(1, 2));
          return null;
        });
    assertThat(
        jdbcRunner.query(
            "select count(*) from table1", PreparedStatementSetter.NOOP, Mappers.SINGLE_INT),
        is(2));

    try {
      jdbcRunner.inTransaction(
          txRunner -> {
            txRunner.execute(INSERT, ps -> ps.setInt(1, 1));
            throw new RuntimeException();
          });
    } catch (RuntimeException ignored) {
    }
    assertThat(
        jdbcRunner.query(
            "select count(*) from table1", PreparedStatementSetter.NOOP, Mappers.SINGLE_INT),
        is(2));
  }

  @Test
  public void nested_in_transction_not_allowed() {
    jdbcRunner.execute("create table table1 ( column1 INT);", PreparedStatementSetter.NOOP);
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> {
              jdbcRunner.inTransaction(
                  txRunner -> {
                    return txRunner.inTransaction(
                        tx2 -> {
                          return null;
                        });
                  });
            });
  }

  private static class TableRowMapper implements RowMapper<Integer> {
    @Override
    public Integer map(ResultSet rs) throws SQLException {
      return rs.getInt("column1");
    }
  }
}
