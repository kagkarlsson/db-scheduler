package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import javax.sql.DataSource;

@FunctionalInterface
public interface DataSourceSupplier {
  /**
   * Supplies an instance of {@link DataSource}.
   *
   * @return The {@link DataSource} to be used by the scheduler.
   */
  DataSource dataSource();
}
