package com.github.kagkarlsson.scheduler.scrolling;

import com.github.kagkarlsson.jdbc.HsqlExtension;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Scrolling integration tests using HSQLDB. This extends the base test class to run all
 * comprehensive scrolling tests against HSQLDB.
 */
public class ScrollingHsqlIntegrationTest extends ScrollingTestBase {

  @RegisterExtension static final HsqlExtension DB = new HsqlExtension();

  @Override
  public DataSource getDataSource() {
    return DB.getDataSource();
  }

  @Override
  protected void performDatabaseSpecificSetup() {
    // Create the scheduled_tasks table with priority column for HSQLDB
    com.github.kagkarlsson.jdbc.JdbcRunner jdbcRunner =
        new com.github.kagkarlsson.jdbc.JdbcRunner(getDataSource());
    jdbcRunner.execute(
        "CREATE TABLE IF NOT EXISTS scheduled_tasks ("
            + "task_name VARCHAR(40) NOT NULL, "
            + "task_instance VARCHAR(40) NOT NULL, "
            + "task_data BLOB, "
            + "execution_time TIMESTAMP NOT NULL, "
            + "picked BOOLEAN NOT NULL, "
            + "picked_by VARCHAR(50), "
            + "last_success TIMESTAMP, "
            + "last_failure TIMESTAMP, "
            + "consecutive_failures INTEGER, "
            + "last_heartbeat TIMESTAMP, "
            + "version BIGINT NOT NULL, "
            + "priority SMALLINT, "
            + "PRIMARY KEY (task_name, task_instance))",
        com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP);
  }
}
