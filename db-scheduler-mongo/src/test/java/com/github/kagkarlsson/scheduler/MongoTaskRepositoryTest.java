package com.github.kagkarlsson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.TaskResolver.UnresolvedTask;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.utils.ExecutionBuilder;
import com.github.kagkarlsson.scheduler.utils.TestUtils;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MongoTaskRepositoryTest {

    @Mock
    private TaskResolver taskResolver;

    @Mock
    private SchedulerName schedulerName;

    @Mock
    private Serializer serializer;

    private MongoTaskRepository repository;

    @RegisterExtension
    protected static EmebddedMongodbExtension emebddedMongodbExtension = new EmebddedMongodbExtension();

    @BeforeEach
    void init() throws IOException {
        // Mock task resolver
        Task taskResolved = Mockito.mock(Task.class);
        Mockito.lenient().when(taskResolved.getDataClass()).thenReturn(SimpleData.class);
        Mockito.lenient().when(taskResolver.resolve(Mockito.anyString()))
            .thenReturn(Optional.of(taskResolved));

        repository = new MongoTaskRepository(taskResolver, schedulerName, serializer,
            "db-scheduler", "db-scheduler", emebddedMongodbExtension.getMongoClient());
    }

    @Test
    void testCreateIfNotExistsOk() {
        Execution execution = new ExecutionBuilder().taskName("idTask").taskInstanceId("idInstance")
            .build();

        boolean created = repository.createIfNotExists(execution);

        assertThat(created).isTrue();

        MongoDatabase db = this.emebddedMongodbExtension.getDb();

        MongoIterable<String> collections = db.listCollectionNames();

        assertThat(collections.iterator().hasNext()).isTrue();
        String collectionName = collections.iterator().next();
        assertThat(collectionName).isEqualTo("db-scheduler");

        // Check presence in collection
        MongoCollection<TaskEntity> collection = this.emebddedMongodbExtension.getCollection();

        TaskEntity taskEntity = collection.find().first();

        assertThat(taskEntity).isNotNull();
        assertThat(taskEntity.getTaskName()).isEqualTo("idTask");
        assertThat(taskEntity.getTaskInstance()).isEqualTo("idInstance");
    }

    @Test
    void testCreateIfNotExistsKo() {
        Execution execution = new ExecutionBuilder().taskName("idTask").taskInstanceId("idInstance")
            .build();

        TaskEntity taskEntityInitial = new TaskEntity();
        taskEntityInitial.setTaskInstance("idInstance");
        taskEntityInitial.setTaskName("idTask");

        this.emebddedMongodbExtension.getCollection().insertOne(taskEntityInitial);

        boolean created = repository.createIfNotExists(execution);
        // Check that no execution is created because it already exists
        assertThat(created).isFalse();
    }

    @Test
    void testGetDueOk() {
        TaskEntityBuilder builder = TaskEntityBuilder.aTaskEntity().picked(false)
            .taskInstance("taskInstance")
            .executionTime(Instant.now().minus(10, ChronoUnit.MINUTES));

        UnresolvedTask unresolvedTask = Mockito.mock(UnresolvedTask.class);
        Mockito.when(unresolvedTask.getTaskName()).thenReturn("unresolved");

        Mockito.when(taskResolver.getUnresolved()).thenReturn(Arrays.asList(unresolvedTask));

        // Insert test data in mongo
        // | Task name  | Task instance | picked | execution time | Should be selected |
        // |------------|---------------|--------|----------------|--------------------|
        // | name-1     | taskInstance  | false  | now - 1m       | true               |
        // | name-2     | taskInstance  | false  | now - 2m       | true               |
        // | name-3     | taskInstance  | false  | now + 5m       | true               |
        // | name-4     | taskInstance  | true   | now            | false              |
        // | unresolved | taskInstance  | false  | now - 1m       | false              |
        List<TaskEntity> entities = Arrays
            .asList(builder.taskName("name-1")
                    .executionTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                    .build(), builder.taskName("name-2")
                    .executionTime(Instant.now().minus(2, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("name-3")
                    .executionTime(Instant.now().plus(5, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("name-4").executionTime(Instant.now()).picked(true).build(),
                builder.taskName("unresolved")
                    .executionTime(Instant.now().minus(1, ChronoUnit.MINUTES)).picked(false)
                    .build());

        // Insert 4 executions, 3 of them are not picked, 2 of those are in the past
        // 1 of them is picked and in the past
        this.emebddedMongodbExtension.getCollection().insertMany(entities);

        // ACT
        List<Execution> executions = repository.getDue(Instant.now(), 10);

        // Check that only name-1 and name-2 are ins the returned list
        assertThat(executions).isNotEmpty();
        assertThat(executions).hasSize(2);
        List<String> taskName = executions.stream().map(e -> e.taskInstance)
            .map(TaskInstance::getTaskName).collect(
                Collectors.toList());

        assertThat(taskName).containsExactly("name-2", "name-1");
    }

    @Test
    void testScheduledExecutions() {
        // Insert test data in mongo
        // | Task name | Task instance | picked | execution time | Should be selected |
        // |-----------|---------------|--------|----------------|--------------------|
        // | task-1    | instance-1    | false  | now + 1m       | yes (order 2)      |
        // | task-1    | instance-2    | false  | now - 1m       | yes (order 1)      |
        // | task-1    | instance-3    | true   | now - 15m      | no                 |
        // | task-2    | instance-1b   | false  | now + 2m       | yes (order 3)      |
        TaskEntityBuilder builder = TaskEntityBuilder.aTaskEntity().picked(false);
        List<TaskEntity> entities = Arrays
            .asList(builder.taskName("task-1").taskInstance("instance-1")
                    .executionTime(Instant.now().plus(1, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-1").taskInstance("instance-2")
                    .executionTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-1").picked(true).taskInstance("instance-3")
                    .executionTime(Instant.now().minus(15, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-2").picked(false).taskInstance("instance-1b")
                    .executionTime(Instant.now().plus(2, ChronoUnit.MINUTES))
                    .build());

        this.emebddedMongodbExtension.getCollection().insertMany(entities);

        ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all().withPicked(false);
        List<Execution> executions = new ArrayList<>();

        // Call method
        repository.getScheduledExecutions(filter, executions::add);

        // Assertions
        assertThat(executions).isNotEmpty();
        assertThat(executions).hasSize(3);

        List<String> taskInstances = executions.stream().map(e -> e.taskInstance)
            .map(TaskInstance::getId)
            .collect(Collectors.toList());

        assertThat(taskInstances).containsExactly("instance-2", "instance-1", "instance-1b");
    }

    @Test
    void testScheduledExecutionsWithPicekd() {
        // Insert test data in mongo
        // | Task name | Task instance | picked | execution time | Should be selected |
        // |-----------|---------------|--------|----------------|--------------------|
        // | task-1    | instance-1    | false  | now + 1m       | no                 |
        // | task-1    | instance-2    | false  | now - 1m       | no                 |
        // | task-1    | instance-3    | true   | now - 15m      | yes                |
        TaskEntityBuilder builder = TaskEntityBuilder.aTaskEntity().picked(false);
        List<TaskEntity> entities = Arrays
            .asList(builder.taskName("task-1").taskInstance("instance-1")
                    .executionTime(Instant.now().plus(1, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-1").taskInstance("instance-2")
                    .executionTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-1").picked(true).taskInstance("instance-3")
                    .executionTime(Instant.now().minus(15, ChronoUnit.MINUTES))
                    .build());

        this.emebddedMongodbExtension.getCollection().insertMany(entities);

        ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all().withPicked(true);
        List<Execution> executions = new ArrayList<>();

        // Call method
        repository.getScheduledExecutions(filter, executions::add);

        // Assertions
        assertThat(executions).isNotEmpty();
        assertThat(executions).hasSize(1);

        List<String> taskInstances = executions.stream().map(e -> e.taskInstance)
            .map(TaskInstance::getId)
            .collect(Collectors.toList());

        assertThat(taskInstances).containsExactly("instance-3");
    }


    @Test
    void testScheduledExecutionsWithTaskName() {
        // Insert test data in mongo
        // | Task name | Task instance | picked | execution time | Should be selected |
        // |-----------|---------------|--------|----------------|--------------------|
        // | task-1    | instance-1    | false  | now + 1m       | yes (order 2)      |
        // | task-1    | instance-2    | false  | now - 1m       | yes (order 1)      |
        // | task-1    | instance-3    | true   | now - 15m      | no                 |
        // | task-2    | instance-1    | false  | now - 15m      | no                 |
        TaskEntityBuilder builder = TaskEntityBuilder.aTaskEntity().picked(false);
        List<TaskEntity> entities = Arrays
            .asList(builder.taskName("task-1").taskInstance("instance-1")
                    .executionTime(Instant.now().plus(1, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-1").taskInstance("instance-2")
                    .executionTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-1").picked(true).taskInstance("instance-3")
                    .executionTime(Instant.now().minus(15, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-2").picked(false).taskInstance("instance-1")
                    .executionTime(Instant.now().minus(15, ChronoUnit.MINUTES))
                    .build());

        this.emebddedMongodbExtension.getCollection().insertMany(entities);
        // Build method parameter for call
        ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all().withPicked(false);
        List<Execution> executions = new ArrayList<>();

        // Call method
        repository.getScheduledExecutions(filter, "task-1", executions::add);

        // Assertions
        assertThat(executions).isNotEmpty();
        assertThat(executions).hasSize(2);

        List<String> taskInstances = executions.stream().map(e -> e.taskInstance)
            .map(TaskInstance::getId)
            .collect(Collectors.toList());

        assertThat(taskInstances).containsExactly("instance-2", "instance-1");
    }

    @Test
    void testScheduledExecutionsWithTaskNameAndPicked() {
        // Insert test data in mongo
        // | Task name | Task instance | picked | execution time | Should be selected |
        // |-----------|---------------|--------|----------------|--------------------|
        // | task-1    | instance-1    | false  | now + 1m       | no                 |
        // | task-1    | instance-2    | true   | now - 1m       | yes                |
        // | task-2    | instance-1    | true   | now - 1m       | no                 |
        TaskEntityBuilder builder = TaskEntityBuilder.aTaskEntity().picked(false);
        List<TaskEntity> entities = Arrays
            .asList(builder.taskName("task-1").taskInstance("instance-1")
                    .executionTime(Instant.now().plus(1, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-1").picked(true).taskInstance("instance-2")
                    .executionTime(Instant.now().minus(15, ChronoUnit.MINUTES))
                    .build(),
                builder.taskName("task-2").picked(true).taskInstance("instance-1")
                    .executionTime(Instant.now().minus(15, ChronoUnit.MINUTES))
                    .build());

        this.emebddedMongodbExtension.getCollection().insertMany(entities);
        // Build method parameter for call
        ScheduledExecutionsFilter filter = ScheduledExecutionsFilter.all().withPicked(true);
        List<Execution> executions = new ArrayList<>();

        // Call method
        repository.getScheduledExecutions(filter, "task-1", executions::add);

        // Assertions
        assertThat(executions).isNotEmpty();
        assertThat(executions).hasSize(1);

        List<String> taskInstances = executions.stream().map(e -> e.taskInstance)
            .map(TaskInstance::getId)
            .collect(Collectors.toList());

        assertThat(taskInstances).containsExactly("instance-2");
    }

    @Test
    void testRemove() {
        // Insert test data in mongo
        // | Task name | Task instance | version | picked | Should be deleted |
        // |-----------|---------------|---------|--------|-------------------|
        // | task-1    | instance-1    | 0       | false  | no                |
        // | task-1    | instance-2    | 1       | true   | yes               |
        TaskEntityBuilder builder = TaskEntityBuilder.aTaskEntity().taskName("task-1");

        List<TaskEntity> entities = Arrays
            .asList(builder.version(0L).picked(false).taskInstance("instance-1").build(),
                builder.version(1).picked(true).taskInstance("instance-2").build());
        this.emebddedMongodbExtension.getCollection().insertMany(entities);

        // Parameter construction
        Execution execution = new ExecutionBuilder().taskName("task-1").taskInstanceId("instance-2")
            .version(1L).build();

        repository.remove(execution);

        List<TaskEntity> tasks = StreamSupport
            .stream(Spliterators.spliteratorUnknownSize(
                this.emebddedMongodbExtension.getCollection().find().iterator(),
                Spliterator.ORDERED), false).collect(Collectors.toList());

        assertThat(tasks).isNotEmpty();
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).getVersion()).isEqualTo(0);
    }

    @Test
    void testRescheduleWithDataOk() {
        // Insert test data in mongo
        TaskEntity task = TaskEntityBuilder.aTaskEntity().taskName("name-1")
            .taskInstance("taskInstance")
            .executionTime(Instant.now()).consecutiveFailures(3).version(12)
            .build();
        this.emebddedMongodbExtension.getCollection().insertOne(task);

        // Build method parameter
        SimpleData dataContent = new SimpleData("content");

        Execution execution = new ExecutionBuilder().taskName("name-1").version(12)
            .taskInstanceId("taskInstance").build();

        // Mock serializer
        byte[] serializedData = dataContent.getDataContent().getBytes(StandardCharsets.UTF_8);
        Mockito.when(serializer.serialize(dataContent))
            .thenReturn(serializedData);

        Instant nextExecutionTime = Instant.now().plus(1, ChronoUnit.HOURS);
        Instant lastSuccess = Instant.now().minus(30, ChronoUnit.MINUTES);
        Instant lastFailure = Instant.now().minus(1, ChronoUnit.HOURS);

        boolean updated = repository
            .reschedule(execution, nextExecutionTime, dataContent, lastSuccess, lastFailure, 4);

        assertThat(updated).isTrue();

        // Retrieve actual object in database
        TaskEntity actual = this.emebddedMongodbExtension.getCollection().find().first();
        // Test if task is correctly updated with parameters
        assertThat(actual).isNotEqualTo(task);
        assertThat(actual.getVersion()).isEqualTo(execution.version + 1);
        assertThat(actual.getExecutionTime())
            .isEqualTo(TestUtils.truncateInstant(nextExecutionTime));
        assertThat(actual.getLastSuccess()).isEqualTo(TestUtils.truncateInstant(lastSuccess));
        assertThat(actual.getLastFailure()).isEqualTo(TestUtils.truncateInstant(lastFailure));
        assertThat(actual.getTaskData()).isEqualTo(serializedData);
        assertThat(actual.getConsecutiveFailures()).isEqualTo(4);
    }

    @Test
    void testRescheduledWithoutData() {
        // Insert test data in mongo
        TaskEntity task = TaskEntityBuilder.aTaskEntity().taskName("name-1")
            .taskInstance("taskInstance")
            .executionTime(Instant.now()).consecutiveFailures(3).version(12)
            .build();
        this.emebddedMongodbExtension.getCollection().insertOne(task);

        // Build method parameter
        Execution execution = new ExecutionBuilder().taskName("name-1").version(12)
            .taskInstanceId("taskInstance").build();

        Instant nextExecutionTime = Instant.now().plus(1, ChronoUnit.HOURS);
        Instant lastSuccess = Instant.now().minus(30, ChronoUnit.MINUTES);
        Instant lastFailure = Instant.now().minus(1, ChronoUnit.HOURS);

        // Execute
        boolean updated = repository
            .reschedule(execution, nextExecutionTime, lastSuccess, lastFailure, 4);

        assertThat(updated).isTrue();
    }

    @Test
    void testNotRescheduled() {
        // Build method parameter
        Execution execution = new ExecutionBuilder().taskName("name-1").version(12)
            .taskInstanceId("taskInstance").build();

        Instant nextExecutionTime = Instant.now().plus(1, ChronoUnit.HOURS);
        Instant lastSuccess = Instant.now().minus(30, ChronoUnit.MINUTES);
        Instant lastFailure = Instant.now().minus(1, ChronoUnit.HOURS);

        // Execute
        boolean updated = repository
            .reschedule(execution, nextExecutionTime, lastSuccess, lastFailure, 4);

        assertThat(updated).isFalse();
    }

    @Test
    void testPickedFound() {
        // Insert test data in mongo
        int version = 0;
        TaskEntity task = TaskEntityBuilder.aTaskEntity().taskName("name-1")
            .taskInstance("taskInstance").executionTime(Instant.now())
            .picked(false).version(version).taskData("test".getBytes()).build();
        this.emebddedMongodbExtension.getCollection().insertOne(task);

        // Build method parameter
        Execution execution = new ExecutionBuilder().taskName("name-1").version(version)
            .taskInstanceId("taskInstance").build();

        Instant timePicked = Instant.now();

        SimpleData simpleData = new SimpleData("data");
        Mockito.when(serializer.deserialize(Mockito.eq(SimpleData.class), Mockito.any()))
            .thenReturn(simpleData);


        Mockito.when(schedulerName.getName()).thenReturn("schedulerName");
        Optional<Execution> executionOpt = repository.pick(execution, timePicked);

        assertThat(executionOpt).isPresent();
        assertThat(executionOpt.get().picked).isTrue();
        assertThat(executionOpt.get().pickedBy).isEqualTo("schedulerName");
        assertThat(executionOpt.get().lastHeartbeat)
            .isEqualTo(timePicked.truncatedTo(ChronoUnit.MILLIS));
        assertThat(executionOpt.get().version).isEqualTo(version + 1);

        assertThat(executionOpt.get().taskInstance.getData()).isNotNull();
        assertThat(executionOpt.get().taskInstance.getData()).isInstanceOf(SimpleData.class);
    }

    @Test
    void testPickedNotFound() {
        // Build method parameter
        Execution execution = new ExecutionBuilder().taskName("name-1").version(0)
            .taskInstanceId("taskInstance").build();

        Mockito.when(schedulerName.getName()).thenReturn("schedulerName");
        Optional<Execution> executionOpt = repository.pick(execution, Instant.now());

        assertThat(executionOpt).isNotPresent();
    }

    @Test
    void testGetDeadExecutions() {
        TaskEntityBuilder shouldBePickedBuilder = TaskEntityBuilder.aTaskEntity().picked(true)
            .taskName("task-1")
            .lastHeartbeat(Instant.now().minus(1, ChronoUnit.HOURS));

        TaskEntityBuilder shouldNotBePickedBuilder = TaskEntityBuilder.aTaskEntity().picked(true)
            .taskName("task-1")
            .lastHeartbeat(Instant.now().minus(10, ChronoUnit.MINUTES));

        // Insert test data in mongo
        // | Task name | Task instance | picked | execution time | Should be selected | Order |
        // |-----------|---------------|--------|----------------|--------------------|-------|
        // | task-1    | instance-1    | true   | now - 1h       | yes                | 2     |
        // | task-1    | instance-2    | true   | now - 2h       | yes                | 1     |
        // | task-1    | instance-3    | true   | now - 10m      | no                 |       |
        // | task-1    | instance-4    | false  | now - 1h       | no                 |       |
        List<TaskEntity> entities = Arrays
            .asList(shouldBePickedBuilder.taskInstance("instance-1").build(),
                shouldBePickedBuilder.taskInstance("instance-2")
                    .lastHeartbeat(Instant.now().minus(2, ChronoUnit.HOURS))
                    .build(),
                shouldNotBePickedBuilder.taskInstance("instance-3").build(),
                shouldNotBePickedBuilder.taskInstance("instance-4").picked(false)
                    .lastHeartbeat(Instant.now().minus(1, ChronoUnit.HOURS))
                    .build());

        this.emebddedMongodbExtension.getCollection().insertMany(entities);

        // Act
        Instant olderThan = Instant.now().minus(30, ChronoUnit.MINUTES);
        List<Execution> actual = repository.getDeadExecutions(olderThan);

        assertThat(actual).hasSize(2);
        assertThat(actual.stream().map(e -> e.taskInstance).map(TaskInstance::getId))
            .containsExactly("instance-2", "instance-1");
    }

    @Test
    void testUpdateHeartbeat() {
        // Build entity to insert in databse
        Instant initialHeartBeat = Instant.now()
            .minus(1, ChronoUnit.HOURS);
        TaskEntity task = TaskEntityBuilder.aTaskEntity().taskName("task-1")
            .taskInstance("instance-1").version(12)
            .lastHeartbeat(initialHeartBeat).build();

        this.emebddedMongodbExtension.getCollection().insertOne(task);

        // Build arguments
        Execution execution = new ExecutionBuilder().taskName("task-1").taskInstanceId("instance-1")
            .version(12).build();
        Instant newHeartbeat = Instant.now();

        repository.updateHeartbeat(execution, newHeartbeat);

        TaskEntity actual = this.emebddedMongodbExtension.getCollection().find().first();
        assertThat(actual).isNotEqualTo(task);
        assertThat(actual.getLastHeartbeat()).isEqualTo(TestUtils.truncateInstant(newHeartbeat));
    }

    @Test
    void testGetExecutionsFailingLongerThan() {
        TaskEntityBuilder taskBuilder = TaskEntityBuilder.aTaskEntity().taskName("task-1")
            .lastFailure(Instant.now());

        // Insert test data in mongo
        // | Task name | Task instance | last hearbeat | last failure | Should be selected |
        // |-----------|---------------|---------------|--------------|--------------------|
        // | task-1    | instance-1    | null          | now          | yes                |
        // | task-1    | instance-2    | now - 1h      | now          | yes                |
        // | task-1    | instance-3    | now - 10m     | now          | no                 |
        // | task-1    | instance-4    | now - 1h      | null         | no                 |
        List<TaskEntity> entities = Arrays.asList(taskBuilder.taskInstance("instance-1").build(),
            taskBuilder.taskInstance("instance-2")
                .lastSuccess(Instant.now().minus(1, ChronoUnit.HOURS)).build(),
            taskBuilder.taskInstance("instance-3")
                .lastSuccess(Instant.now().minus(10, ChronoUnit.MINUTES))
                .build(),
            taskBuilder.taskInstance("instance-4")
                .lastSuccess(Instant.now().minus(1, ChronoUnit.HOURS))
                .lastFailure(null).build());

        this.emebddedMongodbExtension.getCollection().insertMany(entities);

        // Build argument
        Duration interval = Duration.ofMinutes(30);
        List<Execution> actual = repository.getExecutionsFailingLongerThan(interval);

        assertThat(actual).hasSize(2);
        assertThat(actual.stream().map(e -> e.taskInstance).map(TaskInstance::getId))
            .containsExactlyInAnyOrder("instance-1", "instance-2");
    }

    @Test
    void testGetExecutionFound() {
        // Insert data in database
        TaskEntity entity = TaskEntityBuilder.aTaskEntity().taskName("task-1")
            .taskInstance("instance-1").build();

        this.emebddedMongodbExtension.getCollection().insertOne(entity);

        // Call method
        Optional<Execution> actual = repository.getExecution("task-1", "instance-1");

        assertThat(actual).isPresent();
    }

    @Test
    void testGetExecutionNotFound() {
        // Call method
        Optional<Execution> actual = repository.getExecution("task-1", "instance-1");

        assertThat(actual).isNotPresent();
    }

    @Test
    void testRemoveExecutions() {
        TaskEntityBuilder builder = TaskEntityBuilder.aTaskEntity().taskName("task-1");

        List<TaskEntity> entities = Arrays.asList(builder.taskInstance("instance-1").build(),
            builder.taskInstance("instance-2").build(), builder.taskInstance("instance-3").build(),
            builder.taskName("task-2").taskInstance("instance-1").build());
        this.emebddedMongodbExtension.getCollection().insertMany(entities);

        // Call method
        int deletedCount = repository.removeExecutions("task-1");

        assertThat(deletedCount).isEqualTo(3);

        List<TaskEntity> actual = StreamSupport
            .stream(this.emebddedMongodbExtension.getCollection().find().spliterator(), false)
            .collect(Collectors.toList());

        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getTaskName()).isEqualTo("task-2");
    }

    @Test
    void testIndexUniqueness() {
        TaskEntityBuilder builder = TaskEntityBuilder.aTaskEntity().taskInstance("instance")
            .taskName("task");

        List<TaskEntity> entities = Arrays.asList(builder.build(), builder.build());
        MongoBulkWriteException bulkException = Assertions
            .assertThrows(MongoBulkWriteException.class,
                () -> this.emebddedMongodbExtension.getCollection().insertMany(entities));

        MongoWriteException exception = Assertions
            .assertThrows(MongoWriteException.class,
                () -> this.emebddedMongodbExtension.getCollection().insertOne(builder.build()));

        List<TaskEntity> actualEntities = StreamSupport
            .stream(this.emebddedMongodbExtension.getCollection().find().spliterator(), false)
            .collect(Collectors.toList());

        // Check exception
        ErrorCategory category = ErrorCategory.fromErrorCode(exception.getCode());
        assertThat(category).isEqualTo(ErrorCategory.DUPLICATE_KEY);

        // Check that only one document was inserted
        assertThat(bulkException.getWriteResult().getInsertedCount()).isEqualTo(1);
        assertThat(actualEntities).hasSize(1);
    }

    /**
     * Simple object to hold test data
     */
    private class SimpleData {

        private String dataContent;

        public String getDataContent() {
            return dataContent;
        }

        public void setDataContent(String dataContent) {
            this.dataContent = dataContent;
        }

        public SimpleData(String dataContent) {
            this.dataContent = dataContent;
        }
    }
}
