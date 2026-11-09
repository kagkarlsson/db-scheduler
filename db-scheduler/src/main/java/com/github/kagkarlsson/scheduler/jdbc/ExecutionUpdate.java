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

import com.github.kagkarlsson.scheduler.exceptions.ExecutionException;
import com.github.kagkarlsson.scheduler.task.Execution;
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
class ExecutionUpdate {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionUpdate.class);

  @Nullable private final Long versionToUpdate;
  private final TaskInstanceId taskInstance;
  @Nullable private NewValue<byte[]> taskData;
  @Nullable private NewValue<Instant> executionTime;
  @Nullable private NewValue<Boolean> picked;
  @Nullable private NewValue<String> pickedBy;
  @Nullable private NewValue<Instant> lastSuccess;
  @Nullable private NewValue<Instant> lastFailure;
  @Nullable private NewValue<Integer> consecutiveFailures;
  @Nullable private NewValue<Instant> lastHeartbeat;

  ExecutionUpdate(TaskInstanceId taskInstance, Long versionToUpdate) {
    this.taskInstance = taskInstance;
    this.versionToUpdate = versionToUpdate;
  }

  private ExecutionUpdate(TaskInstanceId taskInstance) {
    this.taskInstance = taskInstance;
    this.versionToUpdate = null;
  }

  static ExecutionUpdate forExecution(Execution execution) {
    return new ExecutionUpdate(execution.taskInstance, execution.version);
  }

  static ExecutionUpdate forTaskInstance(TaskInstanceId taskInstance) {
    return new ExecutionUpdate(taskInstance);
  }

  ExecutionUpdate executionTime(@Nullable Instant executionTime) {
    this.executionTime = new NewValue<>(executionTime);
    return this;
  }

  ExecutionUpdate taskData(byte[] taskData) {
    this.taskData = new NewValue<>(taskData);
    return this;
  }

  ExecutionUpdate picked(boolean picked) {
    this.picked = new NewValue<>(picked);
    return this;
  }

  ExecutionUpdate pickedBy(@Nullable String pickedBy) {
    this.pickedBy = new NewValue<>(pickedBy);
    return this;
  }

  ExecutionUpdate lastSuccess(@Nullable Instant lastSuccess) {
    this.lastSuccess = new NewValue<>(lastSuccess);
    return this;
  }

  ExecutionUpdate lastFailure(@Nullable Instant lastFailure) {
    this.lastFailure = new NewValue<>(lastFailure);
    return this;
  }

  ExecutionUpdate consecutiveFailures(@Nullable Integer consecutiveFailures) {
    this.consecutiveFailures = new NewValue<>(consecutiveFailures);
    return this;
  }

  ExecutionUpdate lastHeartbeat(@Nullable Instant lastHeartbeat) {
    this.lastHeartbeat = new NewValue<>(lastHeartbeat);
    return this;
  }

  void updateSingle(JdbcConfig jdbcConfig) {
    var versionToUpdate = throwIfNull(this.versionToUpdate);
    var updatedRows =
        update(jdbcConfig, "version = ?", (ps, index) -> ps.setLong(index, versionToUpdate));
    if (updatedRows != 1) {
      throw new ExecutionException(
          "Expected one execution to be updated, but updated " + updatedRows + ". Indicates a bug.",
          taskInstance.getTaskName(),
          taskInstance.getId(),
          versionToUpdate);
    }
  }

  boolean updateIfNotPicked(JdbcConfig jdbcConfig) {
    return update(jdbcConfig, "picked = ?", (ps, index) -> ps.setBoolean(index, false)) == 1;
  }

  private int update(
      JdbcConfig jdbcConfig, String condition, PreparedStatementParameterSetter conditionSetter) {
    var updates = getUpdates(jdbcConfig);
    var query =
        "UPDATE "
            + jdbcConfig.tableName()
            + " SET "
            + updates.stream().map(u -> u.column + " = ?").collect(joining(", "))
            + (updates.isEmpty() ? "" : ", ")
            + "version = version + 1"
            + " WHERE task_name = ? AND task_instance = ? AND "
            + condition;

    LOG.debug("ExecutionUpdate query: {}", query);
    return jdbcConfig
        .runner()
        .execute(
            query,
            ps -> {
              var index = 1;
              for (var update : updates) {
                update.setter.setParameter(ps, index++);
              }
              ps.setString(index++, taskInstance.getTaskName());
              ps.setString(index++, taskInstance.getId());
              conditionSetter.setParameter(ps, index);
            });
  }

  private List<ColumnUpdate> getUpdates(JdbcConfig jdbcConfig) {
    var updates = new ArrayList<ColumnUpdate>();

    addIfSet(
        executionTime,
        "execution_time",
        updates,
        (ps, index) -> jdbcConfig.customization().setInstant(ps, index, executionTime.value));
    addIfSet(
        taskData,
        "task_data",
        updates,
        (ps, index) -> jdbcConfig.customization().setTaskData(ps, index, taskData.value));
    addIfSet(
        picked, "picked", updates, (ps, index) -> ps.setBoolean(index, toPrimitive(picked.value)));
    addIfSet(pickedBy, "picked_by", updates, (ps, index) -> ps.setString(index, pickedBy.value));
    addIfSet(
        lastSuccess,
        "last_success",
        updates,
        (ps, index) -> jdbcConfig.customization().setInstant(ps, index, lastSuccess.value));
    addIfSet(
        lastFailure,
        "last_failure",
        updates,
        (ps, index) -> jdbcConfig.customization().setInstant(ps, index, lastFailure.value));
    addIfSet(
        consecutiveFailures,
        "consecutive_failures",
        updates,
        (ps, index) -> ps.setInt(index, zeroIfNull(consecutiveFailures.value)));
    addIfSet(
        lastHeartbeat,
        "last_heartbeat",
        updates,
        (ps, index) -> jdbcConfig.customization().setInstant(ps, index, lastHeartbeat.value));
    return updates;
  }

  private void addIfSet(
      @Nullable NewValue<?> field,
      String column,
      List<ColumnUpdate> updates,
      PreparedStatementParameterSetter setter) {
    if (field != null) {
      updates.add(new ColumnUpdate(column, setter));
    }
  }

  private boolean toPrimitive(@Nullable Boolean value) {
    if (value == null) {
      throw new IllegalArgumentException("Value should never be null");
    }
    return value;
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

  record ColumnUpdate(String column, PreparedStatementParameterSetter setter) {}

  interface PreparedStatementParameterSetter {
    void setParameter(PreparedStatement preparedStatement, int index) throws SQLException;
  }

  record NewValue<T>(@Nullable T value) {

    public static <T> NewValue<T> of(T value) {
      return new NewValue<>(value);
    }
  }
}
