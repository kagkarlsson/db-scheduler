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
  //    Enable if test gets flaky!
  //    @RegisterExtension
  //    public ChangeLogLevelsExtension changeLogLevels = new ChangeLogLevelsExtension(
  //        new
  // ChangeLogLevelsExtension.LogLevelOverride("com.github.kagkarlsson.scheduler.DueExecutionsBatch", Level.TRACE),
  //        new ChangeLogLevelsExtension.LogLevelOverride("com.github.kagkarlsson.scheduler.Waiter",
  // Level.DEBUG),
  //        new
  // ChangeLogLevelsExtension.LogLevelOverride("com.github.kagkarlsson.scheduler.Scheduler",
  // Level.DEBUG)
  //    );

  public PostgresqlGenericFetchAndLockCompatibilityTest() {
    super(true);
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
    return Optional.of(new PostgreSqlJdbcCustomization(true));
  }
}
