package com.github.kagkarlsson.scheduler.compatibility;

import static com.github.kagkarlsson.scheduler.helper.TimeHelper.truncatedInstantNow;
import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.SystemClock;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.jdbc.MssqlJdbcCustomization;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.task.SchedulableTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * To test manually, disable testcontainers stuff and use Azure SQL Edge
 *
 * <p>docker run --cap-add SYS_PTRACE -e 'ACCEPT_EULA=1' -e 'MSSQL_SA_PASSWORD=bigSt%rongPwd' -p
 * 1433:1433 --name sqledge -d mcr.microsoft.com/azure-sql-edge
 *
 * <p>String jdbcUrl = "jdbc:sqlserver://localhost:1433"; DataSource datasource = new
 * DriverDataSource(jdbcUrl, "com.microsoft.sqlserver.jdbc.SQLServerDriver", new Properties(), "SA",
 * "bigSt%rongPwd");
 */
@SuppressWarnings("rawtypes")
@Tag("compatibility")
@Testcontainers
public class MssqlCompatibilityTest extends CompatibilityTest {

  @Container
  private static final MSSQLServerContainer MSSQL =
      new MSSQLServerContainer<>(
          DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-latest"));

  private static DataSource pooledDatasource;

  public MssqlCompatibilityTest() {
    super(true, true);
  }

  @BeforeAll
  static void initSchema() {
    //      For MANUAL testing, see javadoc comment
    //        String jdbcUrl = "jdbc:sqlserver://localhost:1433";
    //        DataSource datasource =
    //            new DriverDataSource(
    //                jdbcUrl,
    //                "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    //                new Properties(),
    //                "SA",
    //                "bigSt%rongPwd");
    final DriverDataSource datasource =
        new DriverDataSource(
            MSSQL.getJdbcUrl(),
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            new Properties(),
            MSSQL.getUsername(),
            MSSQL.getPassword());

    //    for debugging SQL
    //            datasource = ProxyDataSourceBuilder.create(datasource)
    //                .logQueryBySlf4j()
    //                .build();

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDataSource(datasource);
    pooledDatasource = new HikariDataSource(hikariConfig);

    // init schema
    DbUtils.dropTables(pooledDatasource);
    DbUtils.runSqlResource("/mssql_tables.sql").accept(pooledDatasource);
  }

  @Override
  public DataSource getDataSource() {
    return pooledDatasource;
  }

  @Override
  public boolean commitWhenAutocommitDisabled() {
    return false;
  }

  /**
   * Verifies that task_data byte[] roundtrips correctly through MSSQL, including odd-length
   * payloads. This failed when task_data was defined as nvarchar(max) because the MSSQL JDBC driver
   * reinterpreted raw bytes as UTF-16LE characters, null-padding odd-length arrays. Fixed by
   * changing the column type to varbinary(max).
   */
  @Test
  void test_task_data_byte_array_roundtrip_with_odd_length() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(20),
        () -> {
          JacksonSerializer serializer = new JacksonSerializer();
          MssqlJdbcCustomization jdbcCustomization = new MssqlJdbcCustomization();
          SystemClock clock = new SystemClock();

          OneTimeTask<String> task =
              TestTasks.oneTimeWithType("byte-roundtrip-task", String.class, (ti, ctx) -> {});

          TaskResolver taskResolver =
              new TaskResolver(SchedulerListeners.NOOP, clock, List.of(task));

          JdbcTaskRepository repository =
              new JdbcTaskRepository(
                  getDataSource(),
                  commitWhenAutocommitDisabled(),
                  jdbcCustomization,
                  DEFAULT_TABLE_NAME,
                  taskResolver,
                  new SchedulerName.Fixed("scheduler1"),
                  serializer,
                  false,
                  clock);

          // "odd" serializes to odd-length JSON
          String taskData = "odd";
          byte[] serializedBytes = serializer.serialize(taskData);
          // Ensure we're actually testing an odd-length payload
          if (serializedBytes.length % 2 == 0) {
            taskData = "odd!";
            serializedBytes = serializer.serialize(taskData);
          }

          final Instant now = truncatedInstantNow();

          TaskInstance<String> taskInstance = task.instance("roundtrip-1", taskData);
          final SchedulableTaskInstance<String> newExecution =
              new SchedulableTaskInstance<>(taskInstance, now);
          repository.createIfNotExists(newExecution);

          // Read raw bytes back from DB and verify they match exactly
          try (Connection conn = getDataSource().getConnection();
              PreparedStatement ps =
                  conn.prepareStatement(
                      "SELECT task_data FROM scheduled_tasks"
                          + " WHERE task_name = ? AND task_instance = ?")) {
            ps.setString(1, "byte-roundtrip-task");
            ps.setString(2, "roundtrip-1");
            try (ResultSet rs = ps.executeQuery()) {
              rs.next();
              byte[] storedBytes = rs.getBytes("task_data");

              assertArrayEquals(
                  serializedBytes,
                  storedBytes,
                  "task_data byte[] roundtrip should be lossless (expected "
                      + serializedBytes.length
                      + " bytes, got "
                      + storedBytes.length
                      + ")");
            }
          }
        });
  }
}
