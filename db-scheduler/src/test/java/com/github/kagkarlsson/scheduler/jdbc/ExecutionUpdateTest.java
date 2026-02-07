package com.github.kagkarlsson.scheduler.jdbc;

import static com.github.kagkarlsson.scheduler.TestTasks.ONETIME_TASK;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.SchedulerName.Fixed;
import com.github.kagkarlsson.scheduler.SystemClock;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.serializer.JavaSerializer;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.State;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ExecutionUpdateTest {

  private static final String TABLE = JdbcTaskRepository.DEFAULT_TABLE_NAME;
  private static final String TASK_ID = "id-1";
  private static final Instant AN_INSTANT = Instant.parse("2020-01-01T12:00:00.00Z");
  private static final Instant ANOTHER_INSTANT = Instant.parse("2022-01-01T12:00:00.00Z");

  private static final OneTimeTask<String> STRING_TASK =
      Tasks.oneTime("string-tas", String.class)
          .execute((TaskInstance<String> taskInstance, ExecutionContext ctx) -> {});

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  private JdbcConfig jdbcConfig;
  private JdbcTaskRepository repository;

  @BeforeEach
  public void setup() {
    jdbcConfig =
        new JdbcConfig(
            TABLE,
            new JdbcRunner(postgres.getDataSource()),
            new PostgreSqlJdbcCustomization(true, false));
    repository =
        new JdbcTaskRepository(
            postgres.getDataSource(),
            false,
            TABLE,
            new TaskResolver(
                new SchedulerListeners(), new SystemClock(), List.of(ONETIME_TASK, STRING_TASK)),
            new Fixed("test"),
            false,
            new SystemClock());
  }

  @Test
  void should_update_execution_time() {
    Execution execution = insert(AN_INSTANT);
    ExecutionUpdate.forExecution(execution).executionTime(ANOTHER_INSTANT).updateSingle(jdbcConfig);

    assertThat(getExecution(execution).executionTime).isEqualTo(ANOTHER_INSTANT);
  }

  @Test
  void should_update_last_success() {
    Execution execution = insert(AN_INSTANT);
    ExecutionUpdate.forExecution(execution).lastSuccess(ANOTHER_INSTANT).updateSingle(jdbcConfig);

    assertThat(getExecution(execution).lastSuccess).isEqualTo(ANOTHER_INSTANT);
  }

  @Test
  void should_update_last_failed() {
    Execution execution = insert(AN_INSTANT);
    ExecutionUpdate.forExecution(execution).lastFailure(ANOTHER_INSTANT).updateSingle(jdbcConfig);

    assertThat(getExecution(execution).lastFailure).isEqualTo(ANOTHER_INSTANT);
  }

  @Test
  void should_update_picked() {
    Execution execution = insert(AN_INSTANT);
    ExecutionUpdate.forExecution(execution).picked(true).updateSingle(jdbcConfig);

    assertThat(getExecution(execution).picked).isTrue();
  }

  @Test
  void should_update_picked_by() {
    Execution execution = insert(AN_INSTANT);
    ExecutionUpdate.forExecution(execution).pickedBy("scheduler-1").updateSingle(jdbcConfig);

    assertThat(getExecution(execution).pickedBy).isEqualTo("scheduler-1");
  }

  @Test
  void should_update_consecutive_failures() {
    Execution execution = insert(AN_INSTANT);
    ExecutionUpdate.forExecution(execution).consecutiveFailures(3).updateSingle(jdbcConfig);

    assertThat(getExecution(execution).consecutiveFailures).isEqualTo(3);
  }

  @Test
  void should_update_last_heartbeat() {
    Execution execution = insert(AN_INSTANT);
    ExecutionUpdate.forExecution(execution).lastHeartbeat(ANOTHER_INSTANT).updateSingle(jdbcConfig);

    assertThat(getExecution(execution).lastHeartbeat).isEqualTo(ANOTHER_INSTANT);
  }

  @Test
  void should_update_state() {
    Execution execution = insert(AN_INSTANT);
    ExecutionUpdate.forExecution(execution).state(State.COMPLETE).updateSingle(jdbcConfig);

    assertThat(getExecution(execution).state).isEqualTo(State.COMPLETE);
  }

  @Test
  void should_update_task_data() {
    var instance =
        STRING_TASK.instanceBuilder(TASK_ID).data("initial-data").scheduledTo(AN_INSTANT);
    repository.createIfNotExists(instance);
    Execution execution = repository.getExecution(instance).orElseThrow();

    byte[] serializedData = new JavaSerializer().serialize("new-data");
    ExecutionUpdate.forExecution(execution).taskData(serializedData).updateSingle(jdbcConfig);

    assertThat(getExecution(execution).taskInstance.getData()).isEqualTo("new-data");
  }

  private Execution insert(Instant executionTime) {
    var instance = ONETIME_TASK.instanceBuilder(TASK_ID).scheduledTo(executionTime);
    repository.createIfNotExists(instance);
    return getExecution(instance);
  }

  private Execution getExecution(TaskInstanceId instance) {
    return repository.getExecution(instance).orElseThrow();
  }
}
