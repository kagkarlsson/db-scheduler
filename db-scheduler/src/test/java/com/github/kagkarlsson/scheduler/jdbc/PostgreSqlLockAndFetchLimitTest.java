package com.github.kagkarlsson.scheduler.jdbc;

import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;
import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.Mappers;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.SystemClock;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * The single-statement lock-and-fetch for PostgreSQL could pick more executions than the limit when
 * the planner chose a nested-loop plan that re-executes the limiting subquery per candidate row.
 * The plan depends on table statistics, so the test forces it deterministically by disabling all
 * alternative planner strategies and recreating the statistics skew that originally triggered it.
 */
class PostgreSqlLockAndFetchLimitTest {

  private static final String FORCE_NESTED_LOOP_OPTIONS =
      "-c enable_hashjoin=off -c enable_mergejoin=off -c enable_material=off"
          + " -c enable_hashagg=off -c enable_sort=off";

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  private JdbcRunner jdbc;
  private JdbcTaskRepository taskRepository;

  @BeforeEach
  void setUp() throws SQLException {
    DataSource forcedPlanDataSource = dataSourceWithForcedNestedLoopPlan();
    jdbc = new JdbcRunner(forcedPlanDataSource);

    Task<Void> task = Tasks.oneTime("lock-and-fetch-limit-task").execute((instance, ctx) -> {});
    var clock = new SystemClock();
    taskRepository =
        new JdbcTaskRepository(
            forcedPlanDataSource,
            false,
            new PostgreSqlJdbcCustomization(false, false),
            DEFAULT_TABLE_NAME,
            new TaskResolver(SchedulerListeners.NOOP, clock, List.of(task)),
            new SchedulerName.Fixed("scheduler1"),
            false,
            clock);
  }

  @Test
  void lockAndGetDue_should_never_pick_more_executions_than_the_limit() {
    analyzeTableWithSingleRowThenReplaceWithDueRows(6);

    List<Execution> picked = taskRepository.lockAndGetDue(Instant.now(), 2);

    assertThat(picked).hasSize(2);
    assertThat(countPickedInDatabase()).isEqualTo(2);
  }

  /**
   * Statistics gathered while the table held one row favour the nested-loop plan that rescans the
   * limiting subquery.
   */
  private void analyzeTableWithSingleRowThenReplaceWithDueRows(int dueRows) {
    insertDueExecution("stats-row");
    jdbc.execute("ANALYZE " + DEFAULT_TABLE_NAME, NOOP);
    jdbc.execute("DELETE FROM " + DEFAULT_TABLE_NAME, NOOP);
    for (int i = 1; i <= dueRows; i++) {
      insertDueExecution("instance-" + i);
    }
  }

  private void insertDueExecution(String instanceId) {
    jdbc.execute(
        "INSERT INTO "
            + DEFAULT_TABLE_NAME
            + " (task_name, task_instance, execution_time, picked, version)"
            + " VALUES ('lock-and-fetch-limit-task', ?, now() - interval '1 second', false, 0)",
        ps -> ps.setString(1, instanceId));
  }

  private int countPickedInDatabase() {
    return jdbc.query(
            "SELECT count(*) FROM " + DEFAULT_TABLE_NAME + " WHERE picked = true",
            NOOP,
            Mappers.SINGLE_INT)
        .intValue();
  }

  /**
   * Disables every planner strategy except nested loop, forcing the plan that triggers the bug.
   * Uses its own non-pooled connections so the options never leak into the shared pooled datasource
   * used by other tests.
   */
  private DataSource dataSourceWithForcedNestedLoopPlan() throws SQLException {
    String url;
    try (var connection = postgres.getDataSource().getConnection()) {
      url = connection.getMetaData().getURL();
    }
    var dataSource = new PGSimpleDataSource();
    dataSource.setUrl(url);
    dataSource.setUser("test");
    dataSource.setProperty("options", FORCE_NESTED_LOOP_OPTIONS);
    return dataSource;
  }
}
