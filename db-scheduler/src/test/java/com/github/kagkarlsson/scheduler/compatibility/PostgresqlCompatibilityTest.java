package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlRule;
import org.junit.Rule;

import javax.sql.DataSource;

public class PostgresqlCompatibilityTest extends CompatibilityTest {

	@Rule
	public EmbeddedPostgresqlRule hsql = new EmbeddedPostgresqlRule(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);

	@Override
	public DataSource getDataSource() {
		return hsql.getDataSource();
	}
}
