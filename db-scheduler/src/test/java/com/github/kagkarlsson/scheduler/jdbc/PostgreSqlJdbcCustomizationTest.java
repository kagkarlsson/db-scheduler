package com.github.kagkarlsson.scheduler.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PostgreSqlJdbcCustomizationTest {
  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  AutodetectJdbcCustomization jdbcCustomization =
      new AutodetectJdbcCustomization(postgres.getDataSource());

  @Test
  void test() {
    assertTrue(jdbcCustomization.supportsExplicitQueryLimitPart());
    Arrays.asList(1, 5, 20, 100)
        .forEach(
            it ->
                assertEquals(
                    " FETCH FIRST " + it + " ROWS ONLY ", jdbcCustomization.getQueryLimitPart(it)));
  }
}
