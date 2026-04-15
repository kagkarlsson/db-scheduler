package com.github.kagkarlsson.scheduler.compatibility;

import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.SystemClock;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.jdbc.OracleJdbcCustomization;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("compatibility")
@Testcontainers
public class Oracle11gUtcTimestampCompatibilityTest extends CompatibilityTest {
  @Container private static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-xe");
  private static HikariDataSource pooledDatasource;

  public Oracle11gUtcTimestampCompatibilityTest() {
    super(false, false);
  }

  @BeforeAll
  static void initSchema() {
    final DriverDataSource datasource =
        new DriverDataSource(
            ORACLE.getJdbcUrl(),
            "oracle.jdbc.OracleDriver",
            new Properties(),
            ORACLE.getUsername(),
            ORACLE.getPassword());

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDataSource(datasource);
    hikariConfig.setMaximumPoolSize(10);
    pooledDatasource = new HikariDataSource(hikariConfig);

    // init schema
    DbUtils.runSqlResource("/oracle_tables_utc.sql", true).accept(pooledDatasource);
  }

  @BeforeEach
  void overrideSchedulerShutdown() {
    stopScheduler.setWaitBeforeInterrupt(Duration.ofMillis(100));
  }

  @Override
  public DataSource getDataSource() {
    return pooledDatasource;
  }

  @Override
  public boolean commitWhenAutocommitDisabled() {
    return false;
  }

}
