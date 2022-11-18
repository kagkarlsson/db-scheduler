# db-scheduler

![build status](https://github.com/kagkarlsson/db-scheduler/workflows/build/badge.svg)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.kagkarlsson/db-scheduler/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.kagkarlsson/db-scheduler)
[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

Task-scheduler for Java that was inspired by the need for a clustered `java.util.concurrent.ScheduledExecutorService` simpler than Quartz.

As such, also appreciated by users ([cbarbosa2](https://github.com/kagkarlsson/db-scheduler/issues/115#issuecomment-649601944), [rafaelhofmann](https://github.com/kagkarlsson/db-scheduler/issues/140#issuecomment-704955500), [BukhariH](https://github.com/kagkarlsson/db-scheduler/pull/268#issue-1147378003)):

> Your lib rocks! I'm so glad I got rid of Quartz and replaced it by yours which is way easier to handle!
>
> [cbarbosa2](https://github.com/cbarbosa2)

See also [why not Quartz?](#why-db-scheduler-when-there-is-quartz)

## Features

* **Cluster-friendly**. Guarantees execution by single scheduler instance.
* **Persistent** tasks. Requires a _single_ database-table for persistence.
* **Embeddable**. Built to be embedded in existing applications.
* **High throughput**. Tested to handle 2k - 10k executions / second. [Link](#benchmark-test).
* **Simple**.
* **Minimal dependencies**. (slf4j)

## Table of contents

* [Getting started](#getting-started)
* [Who uses db-scheduler?](#who-uses-db-scheduler)
* [Examples](#examples)
* [Configuration](#configuration)
* [Third-party task repositories](#third-party-task-repositories)
* [Spring Boot usage](#spring-boot-usage)
* [Interacting with scheduled executions using the SchedulerClient](#interacting-with-scheduled-executions-using-the-schedulerclient)
* [How it works](#how-it-works)
* [Performance](#performance)
* [Versions / upgrading](#versions--upgrading)
* [FAQ](#faq)

## Getting started

1. Add maven dependency
```xml
<dependency>
    <groupId>com.github.kagkarlsson</groupId>
    <artifactId>db-scheduler</artifactId>
    <version>11.6</version>
</dependency>
```

2. Create the `scheduled_tasks` table in your database-schema. See table definition for [postgresql](db-scheduler/src/test/resources/postgresql_tables.sql), [oracle](db-scheduler/src/test/resources/oracle_tables.sql), [mssql](db-scheduler/src/test/resources/mssql_tables.sql) or [mysql](db-scheduler/src/test/resources/mysql_tables.sql).

3. Instantiate and start the scheduler, which then will start any defined recurring tasks.

```java
RecurringTask<Void> hourlyTask = Tasks.recurring("my-hourly-task", FixedDelay.ofHours(1))
        .execute((inst, ctx) -> {
            System.out.println("Executed!");
        });

final Scheduler scheduler = Scheduler
        .create(dataSource)
        .startTasks(hourlyTask)
        .threads(5)
        .build();

// hourlyTask is automatically scheduled on startup if not already started (i.e. exists in the db)
scheduler.start();
```

For more examples, continue reading. For details on the inner workings, see [How it works](#how-it-works). If you have a Spring Boot application, have a look at [Spring Boot Usage](#spring-boot-usage).

## Who uses db-scheduler?

List of organizations known to be running db-scheduler in production:

| Company                                   | Description                                                  |
|-------------------------------------------|--------------------------------------------------------------|
| [Digipost](https://digipost.no)           | Provider of digital mailboxes in Norway                      |
| [Vy Group](https://www.vy.no/en)          | One of the largest transport groups in the Nordic countries. |
| [Wise](https://wise.com/)                 | A cheap, fast way to send money abroad.                      |
| Becker Professional Education             |                                                              |
| [Monitoria](https://monitoria.ca)         | Website monitoring service.                                  |
| [Loadster](https://loadster.app)          | Load testing for web applications.                           |
| [Statens vegvesen](https://www.vegvesen.no/)| The Norwegian Public Roads Administration                  |
| [Lightyear](https://golightyear.com/)     |  A simple and approachable way to invest your money globally.|
| [NAV](https://www.nav.no/)                |  The Norwegian Labour and Welfare Administration             |
| [ModernLoop](https://modernloop.io/)      |  Scale with your company’s hiring needs by using ModernLoop to increase efficiency in interview scheduling, communication, and coordination.             |
| [Diffia](https://www.diffia.com/)         |  Norwegian eHealth company                                   |

Feel free to open a PR to add your organization to the list.

## Examples

See also [runnable examples](https://github.com/kagkarlsson/db-scheduler/tree/master/examples/features/src/main/java/com/github/kagkarlsson/examples).

### Recurring task (_static_)

Define a _recurring_ task and schedule the task's first execution on start-up using the `startTasks` builder-method. Upon completion, the task will be re-scheduled according to the defined schedule (see [pre-defined schedule-types](#schedules)).

```java
RecurringTask<Void> hourlyTask = Tasks.recurring("my-hourly-task", FixedDelay.ofHours(1))
        .execute((inst, ctx) -> {
            System.out.println("Executed!");
        });

final Scheduler scheduler = Scheduler
        .create(dataSource)
        .startTasks(hourlyTask)
        .registerShutdownHook()
        .build();

// hourlyTask is automatically scheduled on startup if not already started (i.e. exists in the db)
scheduler.start();
```

For recurring tasks with multiple instances and schedules, see example [RecurringTaskWithPersistentScheduleMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/RecurringTaskWithPersistentScheduleMain.java).

###  One-time task

An instance of a _one-time_ task has a single execution-time some time in the future (i.e. non-recurring). The instance-id must be unique within this task, and may be used to encode some metadata (e.g. an id). For more complex state, custom serializable java objects are supported (as used in the example).

Define a _one-time_ task and start the scheduler:

```java
OneTimeTask<MyTaskData> myAdhocTask = Tasks.oneTime("my-typed-adhoc-task", MyTaskData.class)
        .execute((inst, ctx) -> {
            System.out.println("Executed! Custom data, Id: " + inst.getData().id);
        });

final Scheduler scheduler = Scheduler
        .create(dataSource, myAdhocTask)
        .registerShutdownHook()
        .build();

scheduler.start();

```

... and then at some point (at runtime), an execution is scheduled using the `SchedulerClient`:

```java
// Schedule the task for execution a certain time in the future and optionally provide custom data for the execution
scheduler.schedule(myAdhocTask.instance("1045", new MyTaskData(1001L)), Instant.now().plusSeconds(5));
```

### More examples

* [EnableImmediateExecutionMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/EnableImmediateExecutionMain.java)
* [MaxRetriesMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/MaxRetriesMain.java)
* [ExponentialBackoffMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/ExponentialBackoffMain.java)
* [ExponentialBackoffWithMaxRetriesMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/ExponentialBackoffWithMaxRetriesMain.java)
* [TrackingProgressRecurringTaskMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/TrackingProgressRecurringTaskMain.java)
* [SpawningOtherTasksMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/SpawningOtherTasksMain.java)
* [SchedulerClientMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/SchedulerClientMain.java)
* [RecurringTaskWithPersistentScheduleMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/RecurringTaskWithPersistentScheduleMain.java)
* [StatefulRecurringTaskWithPersistentScheduleMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/StatefulRecurringTaskWithPersistentScheduleMain.java)
* [JsonSerializerMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/JsonSerializerMain.java)
* [JobChainingUsingTaskDataMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/JobChainingUsingTaskDataMain.java)
* [JobChainingUsingSeparateTasksMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/JobChainingUsingSeparateTasksMain.java)


## Configuration

### Scheduler configuration

The scheduler is created using the `Scheduler.create(...)` builder. The builder has sensible defaults, but the following options are configurable.

#### Consider tuning

:gear: `.threads(int)`<br/>
Number of threads. Default `10`.

:gear: `.pollingInterval(Duration)`<br/>
How often the scheduler checks the database for due executions. Default `10s`.<br/>

:gear: `.enableImmediateExecution()`<br/>
If this is enabled, the scheduler will attempt to directly execute tasks that are scheduled to `now()`, or a time in
the past. For this to work _reliably_, the call to `schedule(..)` should not occur from within a transaction, because
the record may not yet be visible to the scheduler (if this is a requirement, see the
method `scheduler.triggerCheckForDueExecutions()`). Default `false`.

:gear: `.registerShutdownHook()`<br/>
Registers a shutdown-hook that will call `Scheduler.stop()` on shutdown. Stop should always be called for a
graceful shutdown and to avoid dead executions.

:gear: `.shutdownMaxWait(Duration)`<br/>
How long the scheduler will wait before interrupting executor-service threads. If you find yourself using this,
consider if it is possible to instead regularly check `executionContext.getSchedulerState().isShuttingDown()`
in the ExecutionHandler and abort long-running task. Default `30min`.

#### Polling strategy

If you are running >1000 executions/s you might want to use the `lock-and-fetch` polling-strategy for lower overhead
 and higher througput ([read more](#polling-strategy-lock-and-fetch)). If not, the default `fetch-and-lock-on-execute` will be fine.

:gear: `.pollUsingFetchAndLockOnExecute(double, double)`<br/>
Use default polling strategy `fetch-and-lock-on-execute`.<br/>
If the last fetch from the database was a full batch (`executionsPerBatchFractionOfThreads`), a new fetch will be triggered
when the number of executions left are less than or equal to `lowerLimitFractionOfThreads * nr-of-threads`.
Fetched executions are not locked/picked, so the scheduler will compete with other instances for the lock
when it is executed. Supported by all databases.
<br/>Defaults: `0,5, 3.0`


:gear: `.pollUsingLockAndFetch(double, double)`<br/>
Use polling strategy `lock-and-fetch` which uses `select for update .. skip locked` for less overhead.<br/>
If the last fetch from the database was a full batch, a new fetch will be triggered
when the number of executions left are less than or equal to `lowerLimitFractionOfThreads * nr-of-threads`.
The number of executions fetched each time is equal to `(upperLimitFractionOfThreads * nr-of-threads) - nr-executions-left`.
Fetched executions are already locked/picked for this scheduler-instance thus saving one `UPDATE` statement.
<br/>For normal usage, set to for example `0.5, 1.0`.
<br/>For high throughput
(i.e. keep threads busy), set to for example `1.0, 4.0`. Currently hearbeats are not updated for picked executions
in queue (applicable if `upperLimitFractionOfThreads > 1.0`). If they stay there for more than
`4 * hearbeat-interval` (default `20m`), not starting execution, they will be detected as _dead_ and likely be
unlocked again (determined by `DeadExecutionHandler`).  Currently supported by **postgres**.


#### Less commonly tuned

:gear: `.heartbeatInterval(Duration)`<br/>
How often to update the heartbeat timestamp for running executions. Default `5m`.

:gear: `.schedulerName(SchedulerName)`<br/>
Name of this scheduler-instance. The name is stored in the database when an execution is picked by a scheduler.
Default `<hostname>`.

:gear: `.tableName(String)`<br/>
Name of the table used to track task-executions. Change name in the table definitions accordingly when creating
the table. Default `scheduled_tasks`.

:gear: `.serializer(Serializer)`<br/>
Serializer implementation to use when serializing task data. Default to using standard Java serialization,
but db-scheduler also bundles a `GsonSerializer` and `JacksonSerializer`. See examples for a [KotlinSerializer](https://github.com/kagkarlsson/db-scheduler/blob/master/examples/features/src/main/java/com/github/kagkarlsson/examples/kotlin/KotlinSerializer.kt).
See also additional documentation under [Serializers](#Serializers).

:gear: `.executorService(ExecutorService)`<br/>
If specified, use this externally managed executor service to run executions. Ideally the number of threads it
will use should still be supplied (for scheduler polling optimizations). Default `null`.

:gear: `.deleteUnresolvedAfter(Duration)`<br/>
The time after which executions with unknown tasks are automatically deleted. These can typically be old recurring
tasks that are not in use anymore. This is non-zero to prevent accidental removal of tasks through a configuration
error (missing known-tasks) and problems during rolling upgrades. Default `14d`.

:gear: `.jdbcCustomization(JdbcCustomization)`<br/>
db-scheduler tries to auto-detect the database used to see if any jdbc-interactions need to be customized. This
method is an escape-hatch to allow for setting `JdbcCustomizations` explicitly. Default auto-detect.

:gear: `.commitWhenAutocommitDisabled(boolean)`<br/>
By default no commit is issued on DataSource Connections. If auto-commit is disabled, it is assumed that
transactions are handled by an external transaction-manager. Set this property to `true` to override this
behavior and have the Scheduler always issue commits. Default `false`.

:gear: `.failureLogging(Level, boolean)`<br/>
Configures how to log task failures, i.e. `Throwable`s thrown from a task execution handler. Use log level `OFF` to disable
 this kind of logging completely. Default `WARN, true`.

### Task configuration

Tasks are created using one of the builder-classes in `Tasks`. The builders have sensible defaults, but the following options can be overridden.

| Option  | Default | Description |
| ------------- | ---- | ------------- |
| `.onFailure(FailureHandler)`  | see desc.  | What to do when a `ExecutionHandler` throws an exception. By default, _Recurring tasks_ are rescheduled according to their `Schedule` _one-time tasks_ are retried again in 5m. |
| `.onDeadExecution(DeadExecutionHandler)`  | `ReviveDeadExecution`  | What to do when a _dead executions_ is detected, i.e. an execution with a stale heartbeat timestamp. By default dead executions are rescheduled to `now()`. |
| `.initialData(T initialData)`  | `null`  | The data to use the first time a _recurring task_ is scheduled. |


### Schedules

The library contains a number of Schedule-implementations for recurring tasks. See class `Schedules`.

| Schedule  | Description |
| ------------- | ------------- |
| `.daily(LocalTime ...)`  | Runs every day at specified times. Optionally a time zone can be specified. |
| `.fixedDelay(Duration)`  | Next execution-time is `Duration` after last completed execution. **Note:** This `Schedule` schedules the initial execution to `Instant.now()` when used in `startTasks(...)`|
| `.cron(String)`  | Spring-style cron-expression. The pattern `-` is interpreted as a [disabled schedule](#disabled-schedules).  |

Another option to configure schedules is reading string patterns with `Schedules.parse(String)`.

The currently available patterns are:

| Pattern  | Description |
| ------------- | ------------- |
| `FIXED_DELAY\|Ns`  | Same as `.fixedDelay(Duration)` with duration set to N seconds. |
| `DAILY\|12:30,15:30...(\|time_zone)`  | Same as `.daily(LocalTime)` with optional time zone (e.g. Europe/Rome, UTC)|
| `-`  | [Disabled schedule](#disabled-schedules) |

More details on the time zone formats can be found [here](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#of-java.lang.String-).

### Disabled schedules

A `Schedule` can be marked as disabled. The scheduler will not schedule the initial executions for tasks with a disabled schedule,
and it will remove any existing executions for that task.

### Serializers

A task-instance may have some associated data in the field `task_data`. The scheduler uses a `Serializer` to read and write this
data to the database. By default, standard Java serialization is used, but a number of options is provided:

* `GsonSerializer`
* `JacksonSerializer`
* [KotlinSerializer](https://github.com/kagkarlsson/db-scheduler/blob/master/examples/features/src/main/java/com/github/kagkarlsson/examples/kotlin/KotlinSerializer.kt)

For Java serialization it is recommended to specify a `serialVersionUID` to be able to evolve the class representing the data. If not specified,
and the class changes, deserialization will likely fail with a `InvalidClassException`. Should this happen, find and set the current auto-generated
`serialVersionUID` explicitly. It will then be possible to do non-breaking changes to the class.

If you need to migrate from Java serialization to a `GsonSerializer`, configure the scheduler to use a `SerializerWithFallbackDeserializers`:

```java
.serializer(new SerializerWithFallbackDeserializers(new GsonSerializer(), new JavaSerializer()))
```


## Third-party task repositories

Out of the box db-scheduler supports jdbc-compliant databases. There have however been efforts to implement support for more databases via custom task repositories. It is currently a bit cumbersome plugging in a custom repository, but there are plans for making it easier.
This is a list of known third-party task repositories:

* [db-scheduler-mongo](https://github.com/piemjean/db-scheduler-mongo)

## Spring Boot usage

For Spring Boot applications, there is a starter `db-scheduler-spring-boot-starter` making the scheduler-wiring very simple. (See [full example project](https://github.com/kagkarlsson/db-scheduler/tree/master/examples/spring-boot-example)).

### Prerequisites

- An existing Spring Boot application
- A working `DataSource` with schema initialized. (In the example HSQLDB is used and schema is automatically applied.)

### Getting started

1. Add the following Maven dependency
    ```xml
    <dependency>
        <groupId>com.github.kagkarlsson</groupId>
        <artifactId>db-scheduler-spring-boot-starter</artifactId>
        <version>11.6</version>
    </dependency>
    ```
   **NOTE**: This includes the db-scheduler dependency itself.
2. In your configuration, expose your `Task`'s as Spring beans. If they are recurring, they will automatically be picked up and started.
3. If you want to expose `Scheduler` state into actuator health information you need to enable `db-scheduler` health indicator. [Spring Health Information.](https://docs.spring.io/spring-boot/docs/current/reference/html/production-ready-features.html#production-ready-health)
4. Run the app.

### Configuration options

Configuration is mainly done via `application.properties`. Configuration of scheduler-name, serializer and executor-service is done by adding a bean of type `DbSchedulerCustomizer` to your Spring context.

```
# application.properties example showing default values

db-scheduler.enabled=true
db-scheduler.heartbeat-interval=5m
db-scheduler.polling-interval=10s
db-scheduler.polling-limit=
db-scheduler.table-name=scheduled_tasks
db-scheduler.immediate-execution-enabled=false
db-scheduler.scheduler-name=
db-scheduler.threads=10

# Ignored if a custom DbSchedulerStarter bean is defined
db-scheduler.delay-startup-until-context-ready=false

db-scheduler.polling-strategy=fetch
db-scheduler.polling-strategy-lower-limit-fraction-of-threads=0.5
db-scheduler.polling-strategy-upper-limit-fraction-of-threads=3.0

db-scheduler.shutdown-max-wait=30m
```

## Interacting with scheduled executions using the SchedulerClient

It is possible to use the `Scheduler` to interact with the persisted future executions. For situations where a full
`Scheduler`-instance is not needed, a simpler [SchedulerClient](./db-scheduler/src/main/java/com/github/kagkarlsson/scheduler/SchedulerClient.java)
can be created using its builder:

```java
SchedulerClient.Builder.create(dataSource, taskDefinitions).build()
```

It will allow for operations such as:

* List scheduled executions
* Reschedule a specific execution
* Remove an old executions that have been retrying for too long
* ...


## How it works

A single database table is used to track future task-executions. When a task-execution is due, db-scheduler picks it and executes it. When the execution is done, the `Task` is consulted to see what should be done. For example, a `RecurringTask` is typically rescheduled in the future based on its `Schedule`.

The scheduler uses optimistic locking or select-for-update (depending on polling strategy) to guarantee that one and only one scheduler-instance gets to pick and run a task-execution.


### Recurring tasks

The term _recurring task_ is used for tasks that should be run regularly, according to some schedule.

When the execution of a recurring task has finished, a `Schedule` is consulted to determine what the next time for
execution should be, and a future task-execution is created for that time (i.e. it is _rescheduled_).
The time chosen will be the nearest time according to the `Schedule`, but still in the future.

There are two types of recurring tasks, the regular _static_ recurring task, where the `Schedule` is defined statically in the code, and
the _dynamic_ recurring tasks, where the `Schedule` is defined at runtime and persisted in the database (still requiring only a single table).

#### Static recurring task

The _static_ recurring task is the most common one and suitable for regular background jobs since the scheduler automatically schedules
an instance of the task if it is not present and also updates the next execution-time if the `Schedule` is updated.

To create the initial execution for a static recurring task, the scheduler has a method `startTasks(...)` that takes a list of tasks
that should be "started" if they do not already have an existing execution. The initial execution-time is determined by the `Schedule`.
If the task already has a future execution (i.e. has been started at least once before), but an updated `Schedule` now indicates another execution-time,
the existing execution will be rescheduled to the new execution-time (with the exception of _non-deterministic_ schedules
such as `FixedDelay` where new execution-time is further into the future).

Create using `Tasks.recurring(..)`.

#### Dynamic recurring task

The _dynamic_ recurring task is a later addition to db-scheduler and was added to support use-cases where there is need for multiple instances
of the same type of task (i.e. same implementation) with different schedules. The `Schedule` is persisted in the `task_data` alongside any regular data.
Unlike the _static_ recurring task, the dynamic one will not automatically schedule instances of the task. It is up to the user to create instances and
update the schedule for existing ones if necessary (using the `SchedulerClient` interface).
See the example [RecurringTaskWithPersistentScheduleMain.java](./examples/features/src/main/java/com/github/kagkarlsson/examples/RecurringTaskWithPersistentScheduleMain.java) for more details.

Create using `Tasks.recurringWithPersistentSchedule(..)`.

### One-time tasks

The term _one-time task_ is used for tasks that have a single execution-time.
In addition to encode data into the `instanceId`of a task-execution, it is possible to store arbitrary binary data in a separate field for use at execution-time. By default, Java serialization is used to marshal/unmarshal the data.

Create using `Tasks.oneTime(..)`.

### Custom tasks

For tasks not fitting the above categories, it is possible to fully customize the behavior of the tasks using `Tasks.custom(..)`.

Use-cases might be:

* Tasks that should be either rescheduled or removed based on output from the actual execution
* ..


### Dead executions

During execution, the scheduler regularly updates a heartbeat-time for the task-execution. If an execution is marked as executing, but is not receiving updates to the heartbeat-time, it will be considered a _dead execution_ after time X. That may for example happen if the JVM running the scheduler suddenly exits.

When a dead execution is found, the `Task`is consulted to see what should be done. A dead `RecurringTask` is typically rescheduled to `now()`.

## Performance

While db-scheduler initially was targeted at low-to-medium throughput use-cases, it handles high-throughput use-cases (1000+ executions/second) quite well
due to the fact that its data-model is very simple, consisting of a single table of executions.
To understand how it will perform, it is useful to consider the SQL statements it runs per batch of executions.

### Polling strategy fetch-and-lock-on-execute

The original and default polling strategy, `fetch-and-lock-on-execute`, will do the following:
1. `select` a batch of due executions
2. For every execution, on execute, try to `update` the execution to `picked=true` for this scheduler-instance. May miss due to competing schedulers.
3. If execution was picked, when execution is done, `update` or `delete` the record according to handlers.

In sum per batch: 1 select, 2 * batch-size updates   (excluding misses)

### Polling strategy lock-and-fetch

In v10, a new polling strategy (`lock-and-fetch`) was added. It utilizes the fact that most databases now have support for `SKIP LOCKED` in `SELECT FOR UPDATE` statements (see [2ndquadrant blog](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/)).
Using such a strategy, it is possible to fetch executions pre-locked, and thus getting one statement less:

1. `select for update .. skip locked` a batch of due executions. These will already be picked by the scheduler-instance.
3. When execution is done, `update` or `delete` the record according to handlers.

In sum per batch: 1 select-and-update, 1 * batch-size updates   (no misses)


### Benchmark test

To get an idea of what to expect from db-scheduler, see results from the tests run in GCP below.
Tests were run with a few different configurations, but each using 4 competing scheduler-instances running on separate VMs.
TPS is the approx. transactions per second as shown in GCP.

|                                        | Throughput fetch (ex/s) | TPS fetch (estimates) | Throughput lock-and-fetch (ex/s) | TPS lock-and-fetch (estimates) |
|----------------------------------------|------------------|--------------------------|---------------------------|-----------------------------------|
| Postgres 4core 25gb ram, 4xVMs(2-core) |                  |                          |                           |                                   |
| 20 threads, lower 4.0, upper 20.0      | 2000             | 9000                     | 10600                     | 11500                             |
| 100 threads, lower 2.0, upper 6.0      | 2560             | 11000                    | 11200                     | 11200                             |
|                                        |                  |                          |                           |                                   |
| Postgres 8core 50gb ram, 4xVMs(4-core) |                  |                          |                           |                                   |
| 50 threads, lower: 0.5, upper: 4.0     | 4000             | 22000                    | 11840                     | 10300                             |
|                                        |                  |                          |                           |                                   |

Observations for these tests:

* For `fetch-and-lock-on-execute`
  * TPS ≈ 4-5 * execution-throughput. A bit higher than the best-case 2 * execution-throughput, likely due the inefficiency of missed executions.
  * throughput did scale with postgres instance-size, from 2000 executions/s on 4core to 4000 executions/s on 8core
* For `lock-and-fetch`
  * TPS ≈ 1 * execution-throughput. As expected.
  * seem to consistently handle 10k executions/s for these configurations
  * throughput did not scale with postgres instance-size (4-8 core), so bottleneck is somewhere else

Currently, polling strategy `lock-and-fetch` is implemented only for Postgres. Contributions adding support for more databases are welcome.

### User testimonial

There are a number of users that are using db-scheduler for high throughput use-cases. See for example:

* https://github.com/kagkarlsson/db-scheduler/issues/209#issuecomment-1026699872
* https://github.com/kagkarlsson/db-scheduler/issues/190#issuecomment-805867950

### Things to note / gotchas

* There are no guarantees that all instants in a schedule for a `RecurringTask` will be executed. The `Schedule` is consulted after the previous task-execution finishes, and the closest time in the future will be selected for next execution-time. A new type of task may be added in the future to provide such functionality.

* The methods on `SchedulerClient` (`schedule`, `cancel`, `reschedule`) will run using a new `Connection`from the `DataSource` provided. To have the action be a part of a transaction, it must be taken care of by the `DataSource` provided, for example using something like Spring's `TransactionAwareDataSourceProxy`.

* Currently, the precision of db-scheduler is depending on the `pollingInterval` (default 10s) which specifies how often to look in the table for due executions. If you know what you are doing, the scheduler may be instructed at runtime to "look early" via `scheduler.triggerCheckForDueExecutions()`. (See also `enableImmediateExecution()` on the `Builder`)


## Versions / upgrading

See [releases](https://github.com/kagkarlsson/db-scheduler/releases) for release-notes.

**Upgrading to 8.x**
* Custom Schedules must implement a method `boolean isDeterministic()` to indicate whether they will always produce the same instants or not.

**Upgrading to 4.x**
* Add column `consecutive_failures` to the database schema. See table definitions for [postgresql](./db-scheduler/src/test/resources/postgresql_tables.sql), [oracle](./db-scheduler/src/test/resources/oracle_tables.sql) or [mysql](./db-scheduler/src/test/resources/mysql_tables.sql). `null` is handled as 0, so no need to update existing records.

**Upgrading to 3.x**
* No schema changes
* Task creation are preferrably done through builders in `Tasks` class

**Upgrading to 2.x**
* Add column `task_data` to the database schema. See table definitions for [postgresql](./b-scheduler/src/test/resources/postgresql_tables.sql), [oracle](./db-scheduler/src/test/resources/oracle_tables.sql) or [mysql](./db-scheduler/src/test/resources/mysql_tables.sql).

## FAQ

#### Why `db-scheduler` when there is `Quartz`?

The goal of `db-scheduler` is to be non-invasive and simple to use, but still solve the persistence problem, and the cluster-coordination problem.
 It was originally targeted at applications with modest database schemas, to which adding 11 tables would feel a bit overkill..

#### Why use a RDBMS for persistence and coordination?

KISS. It's the most common type of shared state applications have.

#### I am missing feature X?

Please create an issue with the feature request and we can discuss it there.
If you are impatient (or feel like contributing), pull requests are most welcome :)

#### Is anybody using it?

Yes. It is used in production at a number of companies, and have so far run smoothly.
