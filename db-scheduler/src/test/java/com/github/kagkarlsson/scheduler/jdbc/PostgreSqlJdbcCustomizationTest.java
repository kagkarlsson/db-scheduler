package com.github.kagkarlsson.scheduler.jdbc;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgreSqlJdbcCustomizationTest {
    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

    AutodetectJdbcCustomization jdbcCustomization = new AutodetectJdbcCustomization(postgres.getDataSource());

    @Test
    void test() {
        assertTrue(jdbcCustomization.supportsExplicitQueryLimitPart());
        Arrays.asList(1, 5, 20, 100).forEach(
            it -> assertEquals(jdbcCustomization.getQueryLimitPart(it), " LIMIT " + it)
        );
    }
}
