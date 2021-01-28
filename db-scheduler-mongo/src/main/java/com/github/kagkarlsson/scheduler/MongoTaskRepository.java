package com.github.kagkarlsson.scheduler;

import static com.github.kagkarlsson.scheduler.TaskEntity.Fields;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.github.kagkarlsson.scheduler.TaskResolver.UnresolvedTask;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoTaskRepository implements TaskRepository {

    private static final Logger LOG = LoggerFactory.getLogger(MongoTaskRepository.class);
    private final TaskResolver taskResolver;
    private final SchedulerName schedulerSchedulerName;
    private final Serializer serializer;

    private final MongoCollection<TaskEntity> collection;

    public MongoTaskRepository(TaskResolver taskResolver,
        SchedulerName schedulerSchedulerName,
        Serializer serializer, String databaseName, String tableName, MongoClient mongoClient) {
        this.taskResolver = taskResolver;
        this.schedulerSchedulerName = schedulerSchedulerName;
        this.serializer = serializer;

        CodecRegistry pojoCodecRegistry = fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoDatabase db = mongoClient.getDatabase(databaseName)
            .withCodecRegistry(pojoCodecRegistry);

        this.collection = db.getCollection(tableName, TaskEntity.class);

        // Create unique index for (taskName, taskInstance)
        uniqueIndexCreation();
    }

    private void uniqueIndexCreation() {
        LOG.info("Enforce uniqueness");
        IndexOptions indexOption = new IndexOptions();
        indexOption.unique(true);
        Document fields = new Document();
        fields.append(Fields.taskName, 1);
        fields.append(Fields.taskInstance, 1);
        this.collection.createIndex(fields, indexOption);
    }

    @Override
    public boolean createIfNotExists(Execution execution) {
        LOG.debug("Creation request for execution {}", execution);
        // Search criterion : taskName, taskInstance
        final Bson query = buildFilterFromExecution(execution, false);
        Optional<TaskEntity> taskEntityOpt = toEntity(execution);
        if (!taskEntityOpt.isPresent()) {
            return false;
        }
        TaskEntity taskEntity = taskEntityOpt.get();
        taskEntity.setPicked(false);
        taskEntity.setVersion(1L);

        boolean created = false;
        try {
            this.collection.insertOne(taskEntity);
            created = true;
        } catch (MongoWriteException e) {
            LOG.error("Error while saving {} into database", execution, e);
            // Exception is chained only if it is not related to a duplicate key error
            if (!ErrorCategory.fromErrorCode(e.getCode()).equals(ErrorCategory.DUPLICATE_KEY)) {
                throw e;
            }
        }

        // Return true if no object was found
        return created;
    }

    @Override
    public List<Execution> getDue(Instant now, int limit) {
        final Bson pickedFilter = eq(Fields.picked, false);
        final Bson timeFilter = lte(Fields.executionTime, now);
        final Optional<Bson> unresolvedFilter = conditionsForUnresolved();

        final List<Bson> filters = new ArrayList<>();
        filters.add(pickedFilter);
        filters.add(timeFilter);
        unresolvedFilter.ifPresent(filters::add);

        final FindIterable<TaskEntity> tasks = this.collection.find(and(filters))
            .sort(ascending(Fields.executionTime)).limit(limit);

        return StreamSupport.stream(tasks.spliterator(), false).map(this::toExecution)
            .filter(Optional::isPresent).map(Optional::get)
            .collect(Collectors.toList());
    }

    @Override
    public void getScheduledExecutions(
        ScheduledExecutionsFilter filter,
        Consumer<Execution> consumer) {
        LOG.debug("Executions request for {}", filter);
        final Optional<Bson> unresolvedFilter = conditionsForUnresolved();
        final Optional<Bson> executionFilter = conditionForFilter(filter);
        final List<Bson> filters = new ArrayList<>();
        unresolvedFilter.ifPresent(filters::add);
        executionFilter.ifPresent(filters::add);

        final FindIterable<TaskEntity> tasks = this.collection.find(and(filters))
            .sort(ascending(Fields.executionTime));

        StreamSupport.stream(tasks.spliterator(), false).map(this::toExecution)
            .filter(Optional::isPresent).map(Optional::get).forEach(consumer);
    }

    private Optional<Bson> conditionForFilter(ScheduledExecutionsFilter filter) {
        Optional<Bson> bsonFilter = Optional.empty();

        if (filter.getPickedValue().isPresent()) {
            bsonFilter = Optional.of(eq(Fields.picked, filter.getPickedValue().get()));
        }

        return bsonFilter;
    }

    private Optional<Bson> conditionsForUnresolved() {
        List<TaskResolver.UnresolvedTask> unresolvedTasks = this.taskResolver.getUnresolved();
        Optional<Bson> filter = Optional.empty();
        if (!unresolvedTasks.isEmpty()) {
            List<String> unresolvedTaskNames = unresolvedTasks.stream()
                .map(UnresolvedTask::getTaskName).collect(Collectors.toList());
            filter = Optional
                .of(nin(Fields.taskName, unresolvedTaskNames));
        }
        return filter;
    }

    @Override
    public void getScheduledExecutions(
        ScheduledExecutionsFilter filter, String taskName,
        Consumer<Execution> consumer) {

        final Bson taskNameCondition = eq(Fields.taskName, taskName);
        final Optional<Bson> pickedCondition = conditionForFilter(filter);
        final Optional<Bson> unresolvedTaskCondition = conditionsForUnresolved();

        final List<Bson> filters = new ArrayList<>();
        filters.add(taskNameCondition);
        pickedCondition.ifPresent(filters::add);
        unresolvedTaskCondition.ifPresent(filters::add);

        final Bson sortAscExecution = ascending(Fields.executionTime);
        final FindIterable<TaskEntity> tasks = this.collection
            .find(and(filters), TaskEntity.class).sort(sortAscExecution);

        StreamSupport.stream(tasks.spliterator(), false).map(this::toExecution)
            .filter(Optional::isPresent).map(Optional::get).forEach(consumer);
    }

    @Override
    public void remove(Execution execution) {
        final Bson filter = buildFilterFromExecution(execution);

        this.collection.deleteOne(filter);
    }

    /**
     * Build basic search filter from execution taking into account the following fields : -
     * taskName - taskInstance - version
     *
     * @param execution - execution from which task name and task instance are extracted
     * @return Bson filter
     */
    private Bson buildFilterFromExecution(Execution execution) {
        return buildFilterFromExecution(execution, true);
    }

    /**
     * Build basic search filter from execution taking into account the following fields : -
     * taskName - taskInstance - version
     *
     * @param execution   - execution from which task name and task instance are extracted
     * @param withVersion - flag to add version to the filter
     * @return Bson filter
     */
    private Bson buildFilterFromExecution(Execution execution, boolean withVersion) {
        return buildFilterFromParams(execution.taskInstance.getTaskName(),
            execution.taskInstance.getId(), execution.version, withVersion);
    }

    private Bson buildFilterFromParams(String taskName, String taskInstance, Long version,
        boolean withVersion) {
        final Bson taskNameCondition = eq(Fields.taskName, taskName);
        final Bson taskInstanceCondition = eq(Fields.taskInstance, taskInstance);

        List<Bson> filters = new ArrayList<>();
        filters.add(taskNameCondition);
        filters.add(taskInstanceCondition);
        if (withVersion) {
            final Bson taskVersion = eq(Fields.version, version);
            filters.add(taskVersion);
        }

        return and(filters);
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime,
        Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        return rescheduleInternal(execution, nextExecutionTime, null, lastSuccess, lastFailure,
            consecutiveFailures);
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime, Object newData,
        Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        return rescheduleInternal(execution, nextExecutionTime, newData, lastSuccess,
            lastFailure, consecutiveFailures);

    }

    private boolean rescheduleInternal(Execution execution, Instant nextExecutionTime,
        Object data, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {

        final FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.upsert(false);

        Bson update = combine(set(Fields.executionTime, nextExecutionTime),
            set(Fields.taskData, serializer.serialize(data)),
            set(Fields.lastSuccess, lastSuccess), set(Fields.lastFailure, lastFailure),
            set(Fields.consecutiveFailures, consecutiveFailures),
            inc(Fields.version, 1));

        final TaskEntity document = this.collection
            .findOneAndUpdate(buildFilterFromExecution(execution), update, options);

        return !Objects.isNull(document);
    }

    @Override
    public Optional<Execution> pick(Execution e, Instant timePicked) {
        // update where picked is true, task_name, task_instance, version
        final Bson filterNameInstanceVersion = buildFilterFromExecution(e);
        final Bson filters = and(filterNameInstanceVersion, eq(Fields.picked, false));

        final FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.returnDocument(ReturnDocument.AFTER);
        options.upsert(false);

        Bson update = combine(set(Fields.picked, true),
            set(Fields.pickedBy, StringUtils.truncate(schedulerSchedulerName.getName(), 50)),
            set(Fields.lastHeartbeat, timePicked),
            inc(Fields.version, 1));

        TaskEntity document = this.collection.findOneAndUpdate(filters, update, options);

        return toExecution(document);
    }

    @Override
    public List<Execution> getDeadExecutions(Instant olderThan) {
        final List<Bson> filterList = new ArrayList<>();
        filterList.add(eq(Fields.picked, true));
        filterList.add(lte(Fields.lastHeartbeat, olderThan));

        final Optional<Bson> unresolvedFilter = conditionsForUnresolved();
        unresolvedFilter.ifPresent(filterList::add);

        final Bson filters = and(filterList);

        FindIterable<TaskEntity> tasks = this.collection.find(filters)
            .sort(ascending(Fields.lastHeartbeat));

        return StreamSupport.stream(tasks.spliterator(), false).map(this::toExecution)
            .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    @Override
    public void updateHeartbeat(Execution execution, Instant heartbeatTime) {
        final FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.upsert(false);
        final Bson update = set(Fields.lastHeartbeat, heartbeatTime);

        this.collection.findOneAndUpdate(buildFilterFromExecution(execution), update, options);
    }

    @Override
    public List<Execution> getExecutionsFailingLongerThan(Duration interval) {
        final Bson filterFailureNoSuccess = and(ne(Fields.lastFailure, null),
            eq(Fields.lastSuccess, null));
        final Instant boundary = Instant.now().minus(interval);
        final Bson filterFailureOldSuccess = and(ne(Fields.lastFailure, null),
            lt(Fields.lastSuccess, boundary));

        final Bson filters = or(filterFailureNoSuccess, filterFailureOldSuccess);

        FindIterable<TaskEntity> tasks = this.collection.find(filters);

        return StreamSupport.stream(tasks.spliterator(), false).map(this::toExecution)
            .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    @Override
    public Optional<Execution> getExecution(String taskName, String taskInstanceId) {
        final Bson filters = buildFilterFromParams(taskName, taskInstanceId, null, false);

        return toExecution(this.collection.find(filters).first());
    }

    @Override
    public int removeExecutions(String taskName) {
        final Bson filter = eq(Fields.taskName, taskName);

        final DeleteResult deleted = this.collection.deleteMany(filter);

        return Long.valueOf(deleted.getDeletedCount()).intValue();
    }

    /**
     * Mapping method to get an Entity from an Execution
     *
     * @param in - Execution to map
     * @return TaskEntity mapped from execution
     */
    private Optional<TaskEntity> toEntity(Execution in) {
        if (in == null) {
            return Optional.empty();
        }

        TaskEntity out = new TaskEntity();
        Optional<TaskInstance<?>> taskInstanceOpt = Optional
            .ofNullable(in.taskInstance);
        taskInstanceOpt.map(TaskInstance::getTaskName).ifPresent(out::setTaskName);
        taskInstanceOpt.map(TaskInstance::getId).ifPresent(out::setTaskInstance);
        taskInstanceOpt.map(TaskInstance::getData).map(serializer::serialize)
            .ifPresent(out::setTaskData);

        out.setExecutionTime(in.getExecutionTime());
        out.setPicked(in.isPicked());
        out.setPickedBy(in.pickedBy);
        out.setLastFailure(in.lastFailure);
        out.setLastSuccess(in.lastSuccess);
        out.setLastHeartbeat(in.lastHeartbeat);
        out.setVersion(in.version);

        return Optional.of(out);
    }

    /**
     * Mapping method to get an Execution from a TaskEntity
     *
     * @param in - entity to map
     * @return execution mapped
     */
    private Optional<Execution> toExecution(TaskEntity in) {
        if (Objects.isNull(in)) {
            return Optional.empty();
        }

        String taskName = in.getTaskName();
        Optional<Task> task = taskResolver.resolve(taskName);
        Supplier dataSupplier = () -> null;
        if (task.isPresent()) {
            dataSupplier = memoize(
                () -> serializer.deserialize(task.get().getDataClass(), in.getTaskData()));
        }

        TaskInstance taskInstance = new TaskInstance(taskName, in.getTaskInstance(), dataSupplier);

        return Optional
            .of(new Execution(in.getExecutionTime(), taskInstance, in.isPicked(), in.getPickedBy(),
                in.getLastSuccess(), in.getLastFailure(), in.getConsecutiveFailures(),
                in.getLastHeartbeat(), in.getVersion()));
    }

    private static <T> Supplier<T> memoize(Supplier<T> original) {
        return new Supplier<T>() {
            Supplier<T> delegate = this::firstTime;
            boolean initialized;

            public T get() {
                return delegate.get();
            }

            private synchronized T firstTime() {
                if (!initialized) {
                    T value = original.get();
                    delegate = () -> value;
                    initialized = true;
                }
                return delegate.get();
            }
        };
    }
}
