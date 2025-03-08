package com.github.kagkarlsson.scheduler.jdbc;

import static com.github.kagkarlsson.scheduler.jdbc.QueryV2Builder.selectFromTable;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

public class QueryBuilderV2DbTest {

  @RegisterExtension
  public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();
  private JdbcRunner jdbcRunner;

  @BeforeEach
  public void setUp() {
    jdbcRunner = new JdbcRunner(DB.getDataSource());
  }

  @Test
  public void select_no_params() {
    assertEquals(
      selectFromTable("table1", null).generateQuery(),
      "SELECT * FROM table1");
  }

}
