package com.github.kagkarlsson.scheduler.jdbc;

import static com.github.kagkarlsson.scheduler.jdbc.QueryBuilder.selectFromTable;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class QueryBuilderTest {

  @Test
  void test() {
    assertNonWhitespaceEquals("select * from table1", selectFromTable("table1").getQuery());

    assertNonWhitespaceEquals(
        "select * from table1 order by c1 asc",
        selectFromTable("table1").orderBy("c1 asc").getQuery());

    assertNonWhitespaceEquals(
        "select * from table1 where field1=?",
        selectFromTable("table1").andCondition(stringField("field1", "a")).getQuery());

    assertNonWhitespaceEquals(
        "select * from table1 where field1=? and field2=?",
        selectFromTable("table1")
            .andCondition(stringField("field1", "a"))
            .andCondition(stringField("field2", "b"))
            .getQuery());
  }

  private AndCondition stringField(String fieldname, String hasValue) {
    return new AndCondition() {
      @Override
      public String getQueryPart() {
        return fieldname + "=?";
      }

      @Override
      public int setParameters(PreparedStatement p, int index) throws SQLException {
        p.setString(index++, hasValue);
        return index;
      }
    };
  }

  void assertNonWhitespaceEquals(String expected, String actual) {
    assertEquals(normalize(expected), normalize(actual));
  }

  private String normalize(String expected) {
    return expected.replaceAll("\\s+", " ");
  }
}
