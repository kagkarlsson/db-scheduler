package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.RowMapper;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;

public class CustomTableNameTest {

	private static final String SCHEDULER_NAME = "scheduler1";

	private static final String CUSTOM_TABLENAME = "custom_tablename";

	@Rule
	public EmbeddedPostgresqlRule DB = new EmbeddedPostgresqlRule(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);

	private JdbcTaskRepository taskRepository;
	private OneTimeTask<Void> oneTimeTask;

	@Before
	public void setUp() {
		oneTimeTask = TestTasks.oneTime("OneTime", Void.class, TestTasks.DO_NOTHING);
		List<Task<?>> knownTasks = new ArrayList<>();
		knownTasks.add(oneTimeTask);
		taskRepository = new JdbcTaskRepository(DB.getDataSource(), CUSTOM_TABLENAME, new TaskResolver(knownTasks), new SchedulerName.Fixed(SCHEDULER_NAME));

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

	@After
	public void tearDown() {
		new JdbcRunner(DB.getDataSource()).execute("DROP TABLE " + CUSTOM_TABLENAME, NOOP);
	}

}
