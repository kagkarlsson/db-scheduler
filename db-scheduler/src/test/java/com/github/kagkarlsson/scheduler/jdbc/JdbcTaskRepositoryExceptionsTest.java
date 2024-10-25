package com.github.kagkarlsson.scheduler.jdbc;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.PreparedStatementSetter;
import com.github.kagkarlsson.jdbc.ResultSetMapper;
import com.github.kagkarlsson.jdbc.SQLRuntimeException;
import com.github.kagkarlsson.scheduler.SystemClock;
import com.github.kagkarlsson.scheduler.exceptions.ExecutionException;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceException;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JdbcTaskRepositoryExceptionsTest {
  JdbcTaskRepository jdbcTaskRepository;

  @Mock JdbcRunner mockJdbcRunner;

  private String expectedTableName;

  @BeforeEach
  public void setup() {
    expectedTableName = randomAlphanumeric(5);
    jdbcTaskRepository =
        new JdbcTaskRepository(
            null, expectedTableName, null, null, null, mockJdbcRunner, false, new SystemClock());
  }

  @Test
  public void createIfNotExistsFailsToAddNewTask() {
    when(mockJdbcRunner.query(
            ArgumentMatchers.eq(
                "select * from "
                    + expectedTableName
                    + " where task_name = ? and task_instance = ?"),
            any(PreparedStatementSetter.class),
            (ResultSetMapper<List<Execution>>) any(ResultSetMapper.class)))
        .thenReturn(emptyList());
    SQLRuntimeException rootCause = new SQLRuntimeException("SQL GO BOOM!!!");
    when(mockJdbcRunner.execute(
            ArgumentMatchers.startsWith("insert into " + expectedTableName),
            any(PreparedStatementSetter.class)))
        .thenThrow(rootCause);

    TaskInstance<Void> taskInstance =
        new TaskInstance<>(randomAlphanumeric(10), randomAlphanumeric(10));
    SchedulableTaskInstance<Void> execution =
        new SchedulableTaskInstance<>(taskInstance, Instant.now());
    TaskInstanceException actualException =
        assertThrows(
            TaskInstanceException.class,
            () -> {
              jdbcTaskRepository.createIfNotExists(execution);
            });
    assertEquals(
        "Failed to add new execution. (task name: "
            + taskInstance.getTaskName()
            + ", instance id: "
            + taskInstance.getId()
            + ")",
        actualException.getMessage());
    assertEquals(rootCause, actualException.getCause());
    assertEquals(execution.getTaskInstance().getTaskName(), actualException.getTaskName());
    assertEquals(execution.getTaskInstance().getId(), actualException.getInstanceId());
  }

  @Test
  public void getExecutionIsMoreThanOne() {
    TaskInstance<Void> expectedTaskInstance =
        new TaskInstance<>(randomAlphanumeric(10), randomAlphanumeric(10));

    when(mockJdbcRunner.query(
            ArgumentMatchers.eq(
                "select * from "
                    + expectedTableName
                    + " where task_name = ? and task_instance = ?"),
            any(PreparedStatementSetter.class),
            (ResultSetMapper<List<Execution>>) any(ResultSetMapper.class)))
        .thenReturn(
            Lists.newArrayList(
                new Execution(Instant.now(), expectedTaskInstance),
                new Execution(Instant.now(), expectedTaskInstance)));

    TaskInstanceException actualException =
        assertThrows(
            TaskInstanceException.class,
            () -> {
              jdbcTaskRepository.getExecution(expectedTaskInstance);
            });
    assertEquals(
        "Found more than one matching execution for task name/id combination. (task name: "
            + expectedTaskInstance.getTaskName()
            + ", instance id: "
            + expectedTaskInstance.getId()
            + ")",
        actualException.getMessage());
  }

  @ParameterizedTest(name = "Remove ends up removing {0} records")
  @ValueSource(ints = {0, 2})
  public void removesUnexpectedNumberOfRows(int removalCount) {
    when(mockJdbcRunner.execute(
            ArgumentMatchers.eq(
                "delete from "
                    + expectedTableName
                    + " where task_name = ? and task_instance = ? and version = ?"),
            any(PreparedStatementSetter.class)))
        .thenReturn(removalCount);

    TaskInstance<Void> taskInstance =
        new TaskInstance<>(randomAlphanumeric(10), randomAlphanumeric(10));
    Execution execution = new Execution(Instant.now(), taskInstance);
    ExecutionException actualException =
        assertThrows(
            ExecutionException.class,
            () -> {
              jdbcTaskRepository.remove(execution);
            });
    assertEquals(
        "Expected one execution to be removed, but removed "
            + removalCount
            + ". Indicates a bug. (task name: "
            + taskInstance.getTaskName()
            + ", instance id: "
            + taskInstance.getId()
            + ")",
        actualException.getMessage());
    assertEquals(execution.version, actualException.getVersion());
    assertEquals(execution.taskInstance.getTaskName(), actualException.getTaskName());
    assertEquals(execution.taskInstance.getId(), actualException.getInstanceId());
  }

  @ParameterizedTest(name = "Reschedule without new data ends up modifying {0} records")
  @ValueSource(ints = {0, 2})
  public void rescheduleUpdatesUnexpectedNumberOfRowsWithoutNewData(int updateCount) {
    when(mockJdbcRunner.execute(
            ArgumentMatchers.eq(
                "update "
                    + expectedTableName
                    + " set "
                    + "picked = ?, "
                    + "picked_by = ?, "
                    + "last_heartbeat = ?, "
                    + "last_success = ?, "
                    + "last_failure = ?, "
                    + "consecutive_failures = ?, "
                    + "execution_time = ?, "
                    + "version = version + 1 "
                    + "where task_name = ? "
                    + "and task_instance = ? "
                    + "and version = ?"),
            any(PreparedStatementSetter.class)))
        .thenReturn(updateCount);

    TaskInstance<Void> taskInstance =
        new TaskInstance<>(randomAlphanumeric(10), randomAlphanumeric(10));
    Execution execution = new Execution(Instant.now(), taskInstance);
    ExecutionException actualException =
        assertThrows(
            ExecutionException.class,
            () -> {
              jdbcTaskRepository.reschedule(execution, Instant.now(), null, null, 0);
            });
    assertEquals(
        "Expected one execution to be updated, but updated "
            + updateCount
            + ". Indicates a bug. (task name: "
            + taskInstance.getTaskName()
            + ", instance id: "
            + taskInstance.getId()
            + ")",
        actualException.getMessage());
    assertEquals(execution.version, actualException.getVersion());
    assertEquals(execution.taskInstance.getTaskName(), actualException.getTaskName());
    assertEquals(execution.taskInstance.getId(), actualException.getInstanceId());
  }

  @ParameterizedTest(name = "Reschedule with new data ends up modifying {0} records")
  @ValueSource(ints = {0, 2})
  public void rescheduleUpdatesUnexpectedNumberOfRowsWithNewData(int updateCount) {
    when(mockJdbcRunner.execute(
            ArgumentMatchers.eq(
                "update "
                    + expectedTableName
                    + " set "
                    + "picked = ?, "
                    + "picked_by = ?, "
                    + "last_heartbeat = ?, "
                    + "last_success = ?, "
                    + "last_failure = ?, "
                    + "consecutive_failures = ?, "
                    + "execution_time = ?, "
                    + "task_data = ?, "
                    + "version = version + 1 "
                    + "where task_name = ? "
                    + "and task_instance = ? "
                    + "and version = ?"),
            any(PreparedStatementSetter.class)))
        .thenReturn(updateCount);

    TaskInstance<Void> taskInstance =
        new TaskInstance<>(randomAlphanumeric(10), randomAlphanumeric(10));
    Execution execution = new Execution(Instant.now(), taskInstance);
    ExecutionException actualException =
        assertThrows(
            ExecutionException.class,
            () -> {
              jdbcTaskRepository.reschedule(execution, Instant.now(), "", null, null, 0);
            });
    assertEquals(
        "Expected one execution to be updated, but updated "
            + updateCount
            + ". Indicates a bug. (task name: "
            + taskInstance.getTaskName()
            + ", instance id: "
            + taskInstance.getId()
            + ")",
        actualException.getMessage());
    assertEquals(execution.version, actualException.getVersion());
    assertEquals(execution.taskInstance.getTaskName(), actualException.getTaskName());
    assertEquals(execution.taskInstance.getId(), actualException.getInstanceId());
  }
}
