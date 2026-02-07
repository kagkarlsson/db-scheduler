/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.jdbc;

import static java.util.stream.Collectors.joining;

import com.github.kagkarlsson.jdbc.PreparedStatementSetter;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceException;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NullMarked
public class ExecutionUpdate {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionUpdate.class);

  private final Long versionToUpdate;
  private final TaskInstanceId taskInstance;
  @Nullable private NewValue<byte[]> taskData;
  @Nullable private NewValue<Instant> executionTime;
  @Nullable private NewValue<Boolean> picked;
  @Nullable private NewValue<String> pickedBy;
  @Nullable private NewValue<Instant> lastSuccess;
  @Nullable private NewValue<Instant> lastFailure;
  @Nullable private NewValue<Integer> consecutiveFailures;
  @Nullable private NewValue<Instant> lastHeartbeat;
  @Nullable private NewValue<State> state;
  @Nullable private NewValue<Long> version;

  ExecutionUpdate(TaskInstanceId taskInstance, Long versionToUpdate) {
    this.taskInstance = taskInstance;
    this.versionToUpdate = versionToUpdate;
    this.version = new NewValue<>(versionToUpdate + 1);
  }

  public static ExecutionUpdate forExecution(Execution execution) {
    return new ExecutionUpdate(execution.taskInstance, execution.version);
  }

  public ExecutionUpdate executionTime(@Nullable Instant executionTime) {
    this.executionTime = new NewValue<>(executionTime);
    return this;
  }

  public ExecutionUpdate taskData(@Nullable byte[] taskData) {
    this.taskData = new NewValue<>(taskData);
    return this;
  }

  public ExecutionUpdate picked(boolean picked) {
    this.picked = new NewValue<>(picked);
    return this;
  }

  public ExecutionUpdate pickedBy(@Nullable String pickedBy) {
    this.pickedBy = new NewValue<>(pickedBy);
    return this;
  }

  public ExecutionUpdate lastSuccess(@Nullable Instant lastSuccess) {
    this.lastSuccess = new NewValue<>(lastSuccess);
    return this;
  }

  public ExecutionUpdate lastFailure(@Nullable Instant lastFailure) {
    this.lastFailure = new NewValue<>(lastFailure);
    return this;
  }

  public ExecutionUpdate consecutiveFailures(@Nullable Integer consecutiveFailures) {
    this.consecutiveFailures = new NewValue<>(consecutiveFailures);
    return this;
  }

  public ExecutionUpdate lastHeartbeat(@Nullable Instant lastHeartbeat) {
    this.lastHeartbeat = new NewValue<>(lastHeartbeat);
    return this;
  }

  public ExecutionUpdate state(@Nullable State state) {
    this.state = new NewValue<>(state);
    return this;
  }

  public int updateSingle(JdbcConfig jdbcConfig) {
    var setColumns = new ArrayList<String>();
    var setValues = new ArrayList<PreparedStatementParameterSetter>();

    if (executionTime != null) {
      setColumns.add("execution_time");
      setValues.add(
          (ps, index) -> {
            jdbcConfig.customization().setInstant(ps, index, executionTime.value);
          });
    }

    if (taskData != null) {
      setColumns.add("task_data");
      setValues.add(
          (ps, index) -> jdbcConfig.customization().setTaskData(ps, index, taskData.value));
    }

    if (picked != null) {
      setColumns.add("picked");
      setValues.add((ps, index) -> ps.setBoolean(index, picked.value));
    }

    if (pickedBy != null) {
      setColumns.add("picked_by");
      setValues.add((ps, index) -> ps.setString(index, pickedBy.value));
    }

    if (lastSuccess != null) {
      setColumns.add("last_success");
      setValues.add(
          (ps, index) -> {
            jdbcConfig.customization().setInstant(ps, index, lastSuccess.value);
          });
    }

    if (lastFailure != null) {
      setColumns.add("last_failure");
      setValues.add(
          (ps, index) -> jdbcConfig.customization().setInstant(ps, index, lastFailure.value));
    }

    if (consecutiveFailures != null) {
      setColumns.add("consecutive_failures");
      setValues.add((ps, index) -> ps.setInt(index, zeroIfNull(consecutiveFailures.value)));
    }

    if (lastHeartbeat != null) {
      setColumns.add("last_heartbeat");
      setValues.add(
          (ps, index) -> jdbcConfig.customization().setInstant(ps, index, lastHeartbeat.value));
    }

    if (state != null) {
      setColumns.add("state");
      setValues.add(
          (ps, index) -> ps.setString(index, state.value != null ? state.value.name() : null));
    }

    if (version != null) {
      setColumns.add("version");
      setValues.add((ps, index) -> ps.setLong(index, throwIfNull(version.value)));
    }

    if (setColumns.isEmpty()) {
      return 0;
    }

    String query =
        "UPDATE "
            + jdbcConfig.tableName()
            + " SET "
            + setColumns.stream().map(name -> name + " = ?").collect(joining(", "))
            + " WHERE task_name = ? AND task_instance = ? AND version = ?";
    setValues.add((ps, index) -> ps.setString(index, taskInstance.getTaskName()));
    setValues.add((ps, index) -> ps.setString(index, taskInstance.getId()));
    setValues.add((ps, index) -> ps.setLong(index, versionToUpdate));

    LOG.debug("ExecutionUpdate query: {}", query);
    int updatedRows = jdbcConfig.runner().execute(query, toPreparedStatementSetter(setValues));
    if (updatedRows != 1) {
      throw new TaskInstanceException(
          "Expected one execution to be updated, but updated " + updatedRows + ". Indicates a bug.",
          taskInstance.getTaskName(),
          taskInstance.getId());
    }
    return updatedRows;
  }

  private long throwIfNull(@Nullable Long value) {
    if (value != null) {
      return value;
    } else {
      throw new IllegalArgumentException("value cannot be null");
    }
  }

  private int zeroIfNull(@Nullable Integer value) {
    return value != null ? value : 0;
  }

  private PreparedStatementSetter toPreparedStatementSetter(
      List<PreparedStatementParameterSetter> setters) {
    return preparedStatement -> {
      int index = 1;
      for (PreparedStatementParameterSetter setValue : setters) {
        setValue.setParameter(preparedStatement, index++);
      }
    };
  }

  interface PreparedStatementParameterSetter {
    void setParameter(PreparedStatement preparedStatement, int index) throws SQLException;
  }

  record NewValue<T>(@Nullable T value) {

    public static <T> NewValue<T> of(T value) {
      return new NewValue<>(value);
    }
  }
}
