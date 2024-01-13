package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.PostgreSqlJdbcCustomization;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.RegisterExtension;

public class PostgresqlGenericFetchAndLockCompatibilityTest extends CompatibilityTest {

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  public PostgresqlGenericFetchAndLockCompatibilityTest() {
    super(true, true);
  }

  @Override
  public DataSource getDataSource() {
    return postgres.getDataSource();
  }

  @Override
  public boolean commitWhenAutocommitDisabled() {
    return false;
  }

  @Override
  public Optional<JdbcCustomization> getJdbcCustomization() {
    return Optional.of(new PostgreSqlJdbcCustomization(true, false));
  }
}
