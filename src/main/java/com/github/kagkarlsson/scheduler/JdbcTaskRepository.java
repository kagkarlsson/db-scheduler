/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.ResultSetMapper;
import com.github.kagkarlsson.jdbc.SQLRuntimeException;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.github.kagkarlsson.scheduler.StringUtils.truncate;
import static java.util.Optional.ofNullable;

public class JdbcTaskRepository implements TaskRepository {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcTaskRepository.class);
	private static final int MAX_DUE_RESULTS = 10_000;
	private final TaskResolver taskResolver;
	private final SchedulerName schedulerSchedulerName;
	private final JdbcRunner jdbcRunner;
	private Serializer serializer;

	public JdbcTaskRepository(DataSource dataSource, TaskResolver taskResolver, SchedulerName schedulerSchedulerName) {
		this(dataSource, taskResolver, schedulerSchedulerName, Serializer.DEFAULT_JAVA_SERIALIZER);
	}

	public JdbcTaskRepository(DataSource dataSource, TaskResolver taskResolver, SchedulerName schedulerSchedulerName, Serializer serializer) {
		this.taskResolver = taskResolver;
		this.schedulerSchedulerName = schedulerSchedulerName;
		this.jdbcRunner = new JdbcRunner(dataSource);
		this.serializer = serializer;
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public boolean createIfNotExists(Execution execution) {
		try {
			Optional<Execution> existingExecution = getExecution(execution.taskInstance);
			if (existingExecution.isPresent()) {
				LOG.debug("Execution not created, it already exists. Due: {}", existingExecution.get().executionTime);
				return false;
			}

			jdbcRunner.execute(
					"insert into scheduled_tasks(task_name, task_instance, task_data, execution_time, picked, version) values(?, ?, ?, ?, ?, ?)",
					(PreparedStatement p) -> {
						p.setString(1, execution.taskInstance.getTaskName());
						p.setString(2, execution.taskInstance.getId());
						p.setObject(3, serializer.serialize(execution.taskInstance.getData()));
						p.setTimestamp(4, Timestamp.from(execution.executionTime));
						p.setBoolean(5, false);
						p.setLong(6, 1L);
					});
			return true;

		} catch (SQLRuntimeException e) {
			LOG.debug("Exception when inserting execution. Assuming it to be a constraint violation.", e);
			Optional<Execution> existingExecution = getExecution(execution.taskInstance);
			if (!existingExecution.isPresent()) {
				throw new RuntimeException("Failed to add new execution.", e);
			}
			LOG.debug("Execution not created, another thread created it.");
			return false;
		}
	}

	@Override
	public List<Execution> getDue(Instant now) {
		return getDue(now, MAX_DUE_RESULTS);
	}

	public List<Execution> getDue(Instant now, int limit) {
		return jdbcRunner.query(
				"select * from scheduled_tasks where picked = ? and execution_time <= ? order by execution_time asc",
				(PreparedStatement p) -> {
					p.setBoolean(1, false);
					p.setTimestamp(2, Timestamp.from(now));
					p.setMaxRows(limit);
				},
				new ExecutionResultSetMapper()
		);
	}

	@Override
	public void remove(Execution execution) {

		final int removed = jdbcRunner.execute("delete from scheduled_tasks where task_name = ? and task_instance = ? and version = ?",
				ps -> {
					ps.setString(1, execution.taskInstance.getTaskName());
					ps.setString(2, execution.taskInstance.getId());
					ps.setLong(3, execution.version);
				}
		);

		if (removed != 1) {
			throw new RuntimeException("Expected one execution to be removed, but removed " + removed + ". Indicates a bug.");
		}
	}

	@Override
	public void reschedule(Execution execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure) {
		rescheduleInternal(execution, nextExecutionTime, null, lastSuccess, lastFailure);
	}

	@Override
	public void reschedule(Execution execution, Instant nextExecutionTime, Object newData, Instant lastSuccess, Instant lastFailure) {
		rescheduleInternal(execution, nextExecutionTime, new NewData(newData), lastSuccess, lastFailure);
	}

	private void rescheduleInternal(Execution execution, Instant nextExecutionTime, NewData newData, Instant lastSuccess, Instant lastFailure) {
		final int updated = jdbcRunner.execute(
				"update scheduled_tasks set " +
						"picked = ?, " +
						"picked_by = ?, " +
						"last_heartbeat = ?, " +
						"last_success = ?, " +
						"last_failure = ?, " +
						"execution_time = ?, " +
						(newData != null ? "task_data = ?, " : "") +
						"version = version + 1 " +
						"where task_name = ? " +
						"and task_instance = ? " +
						"and version = ?",
				ps -> {
					int index = 1;
					ps.setBoolean(index++, false);
					ps.setString(index++, null);
					ps.setTimestamp(index++, null);
					ps.setTimestamp(index++, ofNullable(lastSuccess).map(Timestamp::from).orElse(null));
					ps.setTimestamp(index++, ofNullable(lastFailure).map(Timestamp::from).orElse(null));
					ps.setTimestamp(index++, Timestamp.from(nextExecutionTime));
					if (newData != null) {
						// may cause datbase-specific problems, might have to use setNull instead
						ps.setObject(index++, serializer.serialize(newData.data));
					}
					ps.setString(index++, execution.taskInstance.getTaskName());
					ps.setString(index++, execution.taskInstance.getId());
					ps.setLong(index++, execution.version);
				});

		if (updated != 1) {
			throw new RuntimeException("Expected one execution to be updated, but updated " + updated + ". Indicates a bug.");
		}
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public Optional<Execution> pick(Execution e, Instant timePicked) {
		final int updated = jdbcRunner.execute(
				"update scheduled_tasks set picked = ?, picked_by = ?, last_heartbeat = ?, version = version + 1 " +
						"where picked = ? " +
						"and task_name = ? " +
						"and task_instance = ? " +
						"and version = ?",
				ps -> {
					ps.setBoolean(1, true);
					ps.setString(2, truncate(schedulerSchedulerName.getName(), 50));
					ps.setTimestamp(3, Timestamp.from(timePicked));
					ps.setBoolean(4, false);
					ps.setString(5, e.taskInstance.getTaskName());
					ps.setString(6, e.taskInstance.getId());
					ps.setLong(7, e.version);
				});

		if (updated == 0) {
			LOG.trace("Failed to pick execution. It must have been picked by another scheduler.", e);
			return Optional.empty();
		} else if (updated == 1) {
			final Optional<Execution> pickedExecution = getExecution(e.taskInstance);
			if (!pickedExecution.isPresent()) {
				throw new IllegalStateException("Unable to find picked execution. Must have been deleted by another thread. Indicates a bug.");
			} else if (!pickedExecution.get().isPicked()) {
				throw new IllegalStateException("Picked execution does not have expected state in database: " + pickedExecution.get());
			}
			return pickedExecution;
		} else {
			throw new IllegalStateException("Updated multiple rows when picking single execution. Should never happen since name and id is primary key. Execution: " + e);
		}
	}

	@Override
	public List<Execution> getOldExecutions(Instant olderThan) {
		return jdbcRunner.query(
				"select * from scheduled_tasks where picked = ? and last_heartbeat <= ? order by last_heartbeat asc",
				(PreparedStatement p) -> {
					p.setBoolean(1, true);
					p.setTimestamp(2, Timestamp.from(olderThan));
				},
				new ExecutionResultSetMapper()
		);
	}

	@Override
	public void updateHeartbeat(Execution e, Instant newHeartbeat) {

		final int updated = jdbcRunner.execute(
				"update scheduled_tasks set last_heartbeat = ? " +
						"where task_name = ? " +
						"and task_instance = ? " +
						"and version = ?",
				ps -> {
					ps.setTimestamp(1, Timestamp.from(newHeartbeat));
					ps.setString(2, e.taskInstance.getTaskName());
					ps.setString(3, e.taskInstance.getId());
					ps.setLong(4, e.version);
				});

		if (updated == 0) {
			LOG.trace("Did not update heartbeat. Execution must have been removed or rescheduled.", e);
		} else {
			if (updated > 1) {
				throw new IllegalStateException("Updated multiple rows updating heartbeat for execution. Should never happen since name and id is primary key. Execution: " + e);
			}
			LOG.debug("Updated heartbeat for execution: " + e);
		}
	}

	@Override
	public List<Execution> getExecutionsFailingLongerThan(Duration interval) {
		return jdbcRunner.query(
				"select * from scheduled_tasks where " +
						"	(last_success is null and last_failure is not null)" +
						"	or (last_failure is not null and last_success < ?)",
				(PreparedStatement p) -> {
					p.setTimestamp(1, Timestamp.from(Instant.now().minus(interval)));
				},
				new ExecutionResultSetMapper()
		);
	}

	public <T> Optional<Execution> getExecution(TaskInstance<T> taskInstance) {
		return getExecution(taskInstance.getTaskName(), taskInstance.getId());
	}

	public Optional<Execution> getExecution(String taskName, String taskInstanceId) {
		final List<Execution> executions = jdbcRunner.query(
				"select * from scheduled_tasks where task_name = ? and task_instance = ?",
				(PreparedStatement p) -> {
					p.setString(1, taskName);
					p.setString(2, taskInstanceId);
				},
				new ExecutionResultSetMapper()
		);
		if (executions.size() > 1) {
			throw new RuntimeException(String.format("Found more than one matching execution for task name/id combination: '%s'/'%s'", taskName, taskInstanceId));
		}

		return executions.size() == 1 ? ofNullable(executions.get(0)) : Optional.empty();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private class ExecutionResultSetMapper implements ResultSetMapper<List<Execution>> {

		@Override
		public List<Execution> map(ResultSet rs) throws SQLException {

			List<Execution> executions = new ArrayList<>();
			while (rs.next()) {
				String taskName = rs.getString("task_name");
				Optional<Task> task = taskResolver.resolve(taskName);

				if (!task.isPresent()) {
					LOG.error("Failed to find implementation for task with name '{}'. Either delete the execution from the databaser, or add an implementation for it.", taskName);
					continue;
				}

				String instanceId = rs.getString("task_instance");
				byte[] data = rs.getBytes("task_data");

				Instant executionTime = rs.getTimestamp("execution_time").toInstant();

				boolean picked = rs.getBoolean("picked");
				final String pickedBy = rs.getString("picked_by");
				Instant lastSuccess = ofNullable(rs.getTimestamp("last_success"))
						.map(Timestamp::toInstant).orElse(null);
				Instant lastFailure = ofNullable(rs.getTimestamp("last_failure"))
						.map(Timestamp::toInstant).orElse(null);
				Instant lastHeartbeat = ofNullable(rs.getTimestamp("last_heartbeat"))
						.map(Timestamp::toInstant).orElse(null);
				long version = rs.getLong("version");

				Supplier dataSupplier = memoize(() -> serializer.deserialize(task.get().getDataClass(), data));
				executions.add(new Execution(executionTime, new TaskInstance(taskName, instanceId, dataSupplier), picked, pickedBy, lastSuccess, lastFailure, lastHeartbeat, version));
			}
			return executions;
		}
	}

	private static <T> Supplier<T> memoize(Supplier<T> original) {
        return new Supplier<T>() {
            Supplier<T> delegate = this::firstTime;
            boolean initialized;
            public T get() {
                return delegate.get();
            }
            private synchronized T firstTime() {
                if(!initialized) {
                    T value = original.get();
                    delegate = () -> value;
                    initialized = true;
                }
                return delegate.get();
            }
        };
    }

    private static class NewData {
		private Object data;
		NewData(Object data) {
			this.data = data;
		}
	}

}
