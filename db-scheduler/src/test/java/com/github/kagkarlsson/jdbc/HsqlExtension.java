package com.github.kagkarlsson.jdbc;

import javax.sql.DataSource;
import org.hsqldb.Database;
import org.hsqldb.DatabaseManager;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class HsqlExtension implements BeforeEachCallback, AfterEachCallback {
  private DataSource dataSource;

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    final JDBCDataSource ds = new JDBCDataSource();
    ds.setUrl("jdbc:hsqldb:mem:jdbcrunner");
    ds.setUser("sa");

    // For jdbc-testing
    //        dataSource = ProxyDataSourceBuilder
    //                .create(ds)
    //                .logQueryBySlf4j()
    //                .methodListener(new TracingMethodListener())
    //                .build();
    dataSource = ds;
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) throws Exception {
    DatabaseManager.closeDatabases(Database.CLOSEMODE_IMMEDIATELY);
  }

  public DataSource getDataSource() {
    return dataSource;
  }
}
