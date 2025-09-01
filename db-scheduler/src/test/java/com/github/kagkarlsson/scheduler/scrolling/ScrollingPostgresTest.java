package com.github.kagkarlsson.scheduler.scrolling;

import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * scrolling integration tests using PostgreSQL database. These tests verify scroll pagination
 * behavior with a real database engine.
 */
public class ScrollingPostgresTest extends ScrollingTestBase {

  @RegisterExtension
  static final EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

  @Override
  public DataSource getDataSource() {
    return DB.getDataSource();
  }
}
