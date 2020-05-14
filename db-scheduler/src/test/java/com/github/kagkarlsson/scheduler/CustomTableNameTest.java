package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.RowMapper;
import com.github.kagkarlsson.scheduler.jdbc.DefaultJdbcCustomization;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;

public class CustomTableNameTest {

    private static final String SCHEDULER_NAME = "scheduler1";

    private static final String CUSTOM_TABLENAME = "custom_tablename";

    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

    private JdbcTaskRepository taskRepository;
    private OneTimeTask<Void> oneTimeTask;

    @BeforeEach
    public void setUp() {
        oneTimeTask = TestTasks.oneTime("OneTime", Void.class, TestTasks.DO_NOTHING);
        List<Task<?>> knownTasks = new ArrayList<>();
        knownTasks.add(oneTimeTask);
        taskRepository = new JdbcTaskRepository(DB.getDataSource(), CUSTOM_TABLENAME, new TaskResolver(StatsRegistry.NOOP, knownTasks), new SchedulerName.Fixed(SCHEDULER_NAME));

        DbUtils.runSqlResource("postgresql_custom_tablename.sql").accept(DB.getDataSource());
    }

    @Test
    public void can_customize_table_name() {
        Instant now = Instant.now();
        TaskInstance<Void> instance1 = oneTimeTask.instance("id1");

        taskRepository.createIfNotExists(new Execution(now, instance1));

        JdbcRunner jdbcRunner = new JdbcRunner(DB.getDataSource());
        jdbcRunner.query("SELECT count(1) AS number_of_tasks FROM " + CUSTOM_TABLENAME, NOOP, (RowMapper<Integer>) rs -> rs.getInt("number_of_tasks"));

    }

    @AfterEach
    public void tearDown() {
        new JdbcRunner(DB.getDataSource()).execute("DROP TABLE " + CUSTOM_TABLENAME, NOOP);
    }

}
