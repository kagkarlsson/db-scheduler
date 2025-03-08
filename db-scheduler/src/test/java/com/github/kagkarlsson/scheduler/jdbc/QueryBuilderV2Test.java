package com.github.kagkarlsson.scheduler.jdbc;

import static com.github.kagkarlsson.scheduler.jdbc.QueryV2Builder.selectFromTable;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.jdbc.PreparedStatementSetter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class QueryBuilderV2Test {

  @BeforeEach
  public void setUp() {}

  @Test
  public void select_no_params() {
    assertEquals(
      selectFromTable("table1", null).generateQuery(),
      "SELECT * FROM table1");
  }

  @Test
  public void select_one_param() {
    assertEquals(
      selectFromTable("table1", null)
        .andCondition("col1=", "val1")
        .generateQuery(),
      "SELECT * FROM table1 WHERE col1=?");
  }

  @Test
  public void select_two_param() {
    assertEquals(
      selectFromTable("table1", null)
        .andCondition("col1=", "val1")
        .andCondition("col2>", "val2")
        .generateQuery(),
      "SELECT * FROM table1 WHERE col1=? AND col2>?");
  }

  @Test
  public void binds_string() throws SQLException {
    PreparedStatementSetter setter = selectFromTable("table1", null)
      .andCondition("col1=", "val1")
      .generatePreparedStatementSetter();

    PreparedStatement ps= Mockito.mock(PreparedStatement.class);
    setter.setParameters(ps);

    Mockito.verify(ps).setString(1, "val1");
  }


}
