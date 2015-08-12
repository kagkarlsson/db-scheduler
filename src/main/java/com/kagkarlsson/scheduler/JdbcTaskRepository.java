package com.kagkarlsson.scheduler;

import com.kagkarlsson.jdbc.IntegrityConstraintViolation;
import com.kagkarlsson.jdbc.JdbcRunner;
import com.kagkarlsson.jdbc.ResultSetMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class JdbcTaskRepository implements TaskRepository {
	private static final Logger LOG = LoggerFactory.getLogger(JdbcTaskRepository.class);
	private final DataSource dataSource;
	private final TaskResolver taskResolver;
	private final JdbcRunner jdbcRunner;

	public JdbcTaskRepository(DataSource dataSource, TaskResolver taskResolver) {
		this.dataSource = dataSource;
		this.taskResolver = taskResolver;
		this.jdbcRunner = new JdbcRunner(dataSource);
	}

	@Override
	public boolean createIfNotExists(Execution execution) {
		try {
			jdbcRunner.execute(
					"insert into scheduled_tasks(task_name, task_instance, execution_time) values (?, ?, ?)",
					(PreparedStatement p) -> {
						p.setString(1, execution.taskInstance.getTask().getName());
						p.setString(2, execution.taskInstance.getTaskId());
						p.setTimestamp(3, Timestamp.valueOf(execution.exeecutionTime));
					});
		} catch (IntegrityConstraintViolation e) {
			LOG.info("Failed to add execution because of existing duplicate: " + execution.taskInstance);
			LOG.trace("Stacktrace", e);
			return false;
		}
		return true;
	}

	@Override
	public List<Execution> getDue(LocalDateTime now) {
		return jdbcRunner.query(
				"select * from scheduled_tasks where execution_time <= ?",
				(PreparedStatement p) -> {
					p.setTimestamp(1, Timestamp.valueOf(now));
				},
				new ExecutionResultSetMapper()
		);
	}

	@Override
	public void remove(Execution execution) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void reschedule(Execution execution, LocalDateTime nextExecutionTime) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public boolean pick(Execution e) {
		throw new UnsupportedOperationException("not implemented");
	}


	/*private class ExecutionRowMapper implements RowMapper<Execution> {

		@Override
		public Execution map(ResultSet rs) throws SQLException {
			String taskName = rs.getString("task_name");
			Task task = taskResolver.resolve(taskName);
			String taskInstance = rs.getString("task_instance");

			LocalDateTime executionTime = rs.getTimestamp("execution_time").toLocalDateTime();
			return new Execution(executionTime, new TaskInstance(task, taskInstance));
		}
	}*/


	private class ExecutionResultSetMapper implements ResultSetMapper<List<Execution>> {
		@Override
		public List<Execution> map(ResultSet rs) throws SQLException {

			List<Execution> executions = new ArrayList<>();
			while(rs.next()) {
				String taskName = rs.getString("task_name");
				Task task = taskResolver.resolve(taskName);

				if (task == null) {
					continue;
				}

				String taskInstance = rs.getString("task_instance");

				LocalDateTime executionTime = rs.getTimestamp("execution_time").toLocalDateTime();
				executions.add(new Execution(executionTime, new TaskInstance(task, taskInstance)));
			}
			return executions;
		}
	}
}
