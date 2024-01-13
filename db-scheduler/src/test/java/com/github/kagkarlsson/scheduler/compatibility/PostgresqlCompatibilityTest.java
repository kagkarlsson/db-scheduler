package com.github.kagkarlsson.scheduler.compatibility;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.RegisterExtension;

public class PostgresqlCompatibilityTest extends CompatibilityTest {

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

  public PostgresqlCompatibilityTest() {
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
}
