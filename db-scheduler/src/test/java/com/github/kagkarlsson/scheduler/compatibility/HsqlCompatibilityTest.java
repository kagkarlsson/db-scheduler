package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.HsqlTestDatabaseRule;
import org.junit.Rule;

import javax.sql.DataSource;

public class HsqlCompatibilityTest extends CompatibilityTest {

	@Rule
	public HsqlTestDatabaseRule hsql = new HsqlTestDatabaseRule();

	@Override
	public DataSource getDataSource() {
		return hsql.getDataSource();
	}

}
