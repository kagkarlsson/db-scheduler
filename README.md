# db-scheduler

![build status](https://travis-ci.org/kagkarlsson/db-scheduler.svg?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.kagkarlsson/db-scheduler/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.kagkarlsson/db-scheduler)
[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

Task-scheduler for Java that was inspired by the need for a clustered `java.util.concurrent.ScheduledExecutorService` simpler than Quartz.

See also [why not Quartz?](#why-db-scheduler-when-there-is-quartz)

## Features

* **Cluster-friendly**. Guarantees execution by single scheduler instance.
* **Persistent** tasks. Requires single database-table for persistence.
* **Embeddable**. Built to be embedded in existing applications.
* **Simple**.
* **Minimal dependencies**. (slf4j)

## Getting started

1. Add maven dependency
```xml
<dependency>
    <groupId>com.github.kagkarlsson</groupId>
    <artifactId>db-scheduler</artifactId>
    <version>7.1</version>
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

## Examples

### Recurring task

Define a _recurring_ task and schedule the task's first execution on start-up using the `startTasks` builder-method. Upon completion, the task will be re-scheduled according to the defined schedule (see [pre-defined schedule-types](#schedules)).

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


###  One-time tasks

An instance of a _one-time_ task has a single execution-time some time in the future (i.e. non-recurring). The instance-id must be unique within this task, and may be used to encode some metadata (e.g. an id). For more complex state, custom serializable java objects are supported (as used in the example).

Define a _one-time_ task and start the scheduler:

```java
OneTimeTask<MyTaskData> myAdhocTask = Tasks.oneTime("my-typed-adhoc-task", MyTaskData.class)
        .execute((inst, ctx) -> {
            System.out.println("Executed! Custom data, Id: " + inst.getData().id);
        });

final Scheduler scheduler = Scheduler
        .create(dataSource, myAdhocTask)
        .threads(5)
        .build();

scheduler.start();

```

... and then at some point (at runtime), an execution is scheduled using the `SchedulerClient`:

```java
// Schedule the task for execution a certain time in the future and optionally provide custom data for the execution
scheduler.schedule(myAdhocTask.instance("1045", new MyTaskData(1001L)), Instant.now().plusSeconds(5));
```


### Proper shutdown of the scheduler

To avoid unnecessary [dead exexutions](#dead-executions), it is important to shutdown the scheduler properly, i.e. calling the `shutdown` method.

```java

final Scheduler scheduler = Scheduler
        .create(dataSource, myAdhocTask)
        .build();

Runtime.getRuntime().addShutdownHook(new Thread() {
    @Override
    public void run() {
        LOG.info("Received shutdown signal.");
        scheduler.stop();
    }
});

scheduler.start();
```

## Configuration

### Scheduler configuration

The scheduler is created using the `Scheduler.create(...)` builder. The builder have sensible defaults, but the following options are configurable.

| Option  | Default | Description |
| ------------- | ---- | ------------- |
| `.threads(int)`  | 10  | Number of threads |
| `.pollingInterval(Duration)`  |  30s  | How often the scheduler checks the database for due executions. |
| `.pollingLimit(int)`  |  3 * `<nr-of-threads>`  | Maximum number of executions to fetch on a check for due executions. |
| `.heartbeatInterval(Duration)`  | 5m | How often to update the heartbeat timestamp for running executions. |
| `.schedulerName(SchedulerName)`  | hostname  | Name of this scheduler-instance. The name is stored in the database when an execution is picked by a scheduler. |
| `.tableName(String)`  | `scheduled_tasks` | Name of the table used to track task-executions. Change name in the table definitions accordingly when creating the table. |
| `.serializer(Serializer)`  | standard Java | Serializer implementation to use when serializing task data. |
| `.enableImmediateExecution()`  | false | If this is enabled, the scheduler will attempt to directly execute tasks that are scheduled to `now()`, or a time in the past. For this to work, the call to `schedule(..)` must not occur from within a transaction, because the record will not yet be visible to the scheduler (if this is a requirement, see the method `scheduler.triggerCheckForDueExecutions()`) |
| `.executorService(ExecutorService)`  | `null`  | If specified, use this externally managed executor service to run executions. Ideally the number of threads it will use should still be supplied (for scheduler polling optimizations). |
| `.deleteUnresolvedAfter(Duration)`  | `14d`  | The time after which executions with unknown tasks are automatically deleted. These can typically be old recurring tasks that are not in use anymore. This is non-zero to prevent accidental removal of tasks through a configuration error (missing known-tasks) and problems during rolling upgrades. |
| `.jdbcCustomization(JdbcCustomization)`  | auto  | db-scheduler tries to auto-detect the database used to see if any jdbc-interactions need to be customized. This method is an escape-hatch to allow for setting `JdbcCustomizations` explicitly. |



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
| `.cron(String)`  | Spring-style cron-expression. |

Another option to configure schedules is reading string patterns with `Schedules.parse(String)`.

The currently available patterns are:

| Pattern  | Description |
| ------------- | ------------- |
| `FIXED_DELAY\|Ns`  | Same as `.fixedDelay(Duration)` with duration set to N seconds. |
| `DAILY\|12:30,15:30...(\|time_zone)`  | Same as `.daily(LocalTime)` with optional time zone (e.g. Europe/Rome, UTC)|

More details on the time zone formats can be found [here](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#of-java.lang.String-).

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
        <version>7.1</version>
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
db-scheduler.polling-interval=30s
db-scheduler.polling-limit=
db-scheduler.table-name=scheduled_tasks
db-scheduler.immediate-execution-enabled=false
db-scheduler.scheduler-name=
db-scheduler.threads=10
# Ignored if a custom DbSchedulerStarter bean is defined
db-scheduler.delay-startup-until-context-ready=false
```


## How it works

A single database table is used to track future task-executions. When a task-execution is due, db-scheduler picks it and executes it. When the execution is done, the `Task` is consulted to see what should be done. For example, a `RecurringTask` is typically rescheduled in the future based on its `Schedule`.

Optimistic locking is used to guarantee that a one and only one scheduler-instance gets to pick a task-execution.


### Recurring tasks

The term _recurring task_ is used for tasks that should be run regularly, according to some schedule (see ``Tasks.recurring(..)``).

When the execution of a recurring task has finished, a `Schedule` is consulted to determine what the next time for execution should be, and a future task-execution is created for that time (i.e. it is _rescheduled_). The time chosen will be the nearest time according to the `Schedule`, but still in the future.

To create the initial execution for a `RecurringTask`, the scheduler has a method  `startTasks(...)` that takes a list of tasks that should be "started" if they do not already have a future execution.

### One-time tasks

The term _one-time task_ is used for tasks that have a single execution-time (see `Tasks.oneTime(..)`).
In addition to encode data into the `instanceId`of a task-execution, it is possible to store arbitrary binary data in a separate field for use at execution-time. By default, Java serialization is used to marshal/unmarshal the data.

### Custom tasks

For tasks not fitting the above categories, it is possible to fully customize the behavior of the tasks using `Tasks.custom(..)`.

Use-cases might be:

* Recurring tasks that needs to update its data
* Tasks that should be either rescheduled or removed based on output from the actual execution


### Dead executions

During execution, the scheduler regularly updates a heartbeat-time for the task-execution. If an execution is marked as executing, but is not receiving updates to the heartbeat-time, it will be considered a _dead execution_ after time X. That may for example happen if the JVM running the scheduler suddenly exits.

When a dead execution is found, the `Task`is consulted to see what should be done. A dead `RecurringTask` is typically rescheduled to `now()`.


### Things to note / gotchas

* There are no guarantees that all instants in a schedule for a `RecurringTask` will be executed. The `Schedule` is consulted after the previous task-execution finishes, and the closest time in the future will be selected for next execution-time. A new type of task may be added in the future to provide such functionality.

* The methods on `SchedulerClient` (`schedule`, `cancel`, `reschedule`) and the `CompletionHandler` will run using a new `Connection`from the `DataSource`provided. To have the action be a part of a transaction, it must be taken care of by the `DataSource`provided, for example using something like Spring's `TransactionAwareDataSourceProxy`.

* Currently, the precision of db-scheduler is depending on the `pollingInterval` (default 10s) which specifies how often to look in the table for due executions. If you know what you are doing, the scheduler may be instructed at runtime to "look early" via `scheduler.triggerCheckForDueExecutions()`. (See also `enableImmediateExecution()` on the `Builder`)


## Versions / upgrading

### Version 7.1
* PR [#109](https://github.com/kagkarlsson/db-scheduler/pull/109) fixes db-scheduler for data sources returning connections where `autoCommit=false`. db-scheduler will now issue an explicit `commit` for these cases.

### Version 7.0
* PR [#105](https://github.com/kagkarlsson/db-scheduler/pull/105) fixes bug for `Microsoft Sql Server` where incorrect timezone handling caused persisted instant != read instant.
  This bug was discovered when adding testcontainers-based compatibility tests and has strangely enough never been reported by users. So this release will cause a change
  in behavior for users where the database is discovered to be `Microsoft SQL Server`.

### Version 6.8
* PR [#96](https://github.com/kagkarlsson/db-scheduler/pull/96) allow for overriding `DbSchedulerStarter` in Spring Boot starter. Contributed by [evenh](https://github.com/evenh).
* Upgraded to JUnit 5
* Full indentation reformatting of the codebase due to mix of tabs and spaces.

### Version 6.7
* PR [#87](https://github.com/kagkarlsson/db-scheduler/pull/87) allow for specifying the TimeZone for the Daily schedule and the Schedule string-parser (contributed by [alex859](https://github.com/alex859))
* PR [#90](https://github.com/kagkarlsson/db-scheduler/pull/90) adds task-name to logging of failures (contributed by [alex859](https://github.com/alex859))

### Version 6.6
* PR [#86](https://github.com/kagkarlsson/db-scheduler/pull/85) changes Spring Boot `HealthIndicator` to opt-in rather than default on. (contributed by [ystarikovich](https://github.com/ystarikovich))

### Version 6.5
* PR [#83](https://github.com/kagkarlsson/db-scheduler/pull/83) added additional exclusions of executions with unresolved task names to `getScheduledExecutions()` and `getExecutionsFailingLongerThan(..)`.
* PR [#82](https://github.com/kagkarlsson/db-scheduler/pull/82) sets junit to test-scope in `db-scheduler-boot-starter` `pom.xml`. (contributed by [ystarikovich](https://github.com/ystarikovich))

### Version 6.4
* Added configuration option from version 6.3 (`deleteUnresolvedAfter(Duration)`) to Spring Boot starter.

### Version 6.3
* PR [#80](https://github.com/kagkarlsson/db-scheduler/pull/80) adds more graceful handling of unresolved tasks. Executions with unknown tasks will not (in extreme cases) be able to block other executions. They will also automatically be removed from the database after a duration controlled by builder-method `deleteUnresolvedAfter(Duration)`, which currently defaults to 14d.

### Version 6.2
* PR [#71](https://github.com/kagkarlsson/db-scheduler/pull/71) allows for configuring Spring to delay starting the scheduler  until after context is fully started. (contributed by [evenh](https://github.com/evenh))

### Version 6.1
* PR [#68](https://github.com/kagkarlsson/db-scheduler/pull/68) allows for specifying time-zone for cron-schedules (contributed by [paulhilliar](https://github.com/paulhilliar))

### Version 6.0
* PR [#63](https://github.com/kagkarlsson/db-scheduler/pull/63) adds Spring Boot support. Scheduler can now be autoconfigured using tasks available in the Spring context. (contributed by [evenh](https://github.com/evenh))

### Version 5.2
* PR [#60](https://github.com/kagkarlsson/db-scheduler/pull/60) changes `RecurringTask` so that initial/first execution-time is defined in the `Schedule` and typically is the next Instant according to the Schedule.

### Version 5.1
* PR [#52](https://github.com/kagkarlsson/db-scheduler/pull/52) redesigns use of the underlying `ExecutorService`, making better use of the backing queue.
* PR [#53](https://github.com/kagkarlsson/db-scheduler/pull/53) adds a method to the `SchedulerClient` for checking if a `TaskInstance` already exists, `client.getScheduledExecution(<task-instance-id>)` (fixes [#38](https://github.com/kagkarlsson/db-scheduler/issues/38).
* PR [#54](https://github.com/kagkarlsson/db-scheduler/pull/54) adds a builder-method for supplying an externally managed `ExecutorService` (fixes [#51](https://github.com/kagkarlsson/db-scheduler/issues/51)).
* PR [#56](https://github.com/kagkarlsson/db-scheduler/pull/56) adds cron-support, `Schedules.cron(<pattern>)` (fixes [#40](https://github.com/kagkarlsson/db-scheduler/issues/40)).

### Version 5.0
* PR #47 allows for setting max number of executions fetched by the scheduler (contributed by [bgooren](https://github.com/bgooren))
* PR #48 fixes a bug for medium-sized volumes where the scheduler would not continue to poll for executions until there were none left (contributed by [bgooren](https://github.com/bgooren))

### Version 4.1
* Helper for using a version of the scheduler in unit/integration tests is now available in the artifact, through the class `TestHelper.createManualScheduler(...)`. For usage example see `SchedulerClientTest`.
* It is now possible to manually trigger a check for due executions in the database. Of course, if this is done too frequently there will be an increased overhead.
* The scheduler can be instructed to do a best-effort attempt at executing executions it sees is being scheduled to run `now()` or earlier through the builder-method `enableImmediateExecution()`.
* Bugfix: `scheduler.getScheduledExecutionsForTask(...)` was not working properly

### Version 4.0
* Track number of consecutive failures of a task. For use in `FailureHandler` to avoid retrying forever, or retry with back-off.

**Upgrading to 4.x**
* Add column `consecutive_failures` to the database schema. See table definitions for [postgresql](db-scheduler/src/test/resources/postgresql_tables.sql), [oracle](https://github.com/kagkarlsson/db-scheduler/src/test/resources/oracle_tables.sql) or [mysql](https://github.com/kagkarlsson/db-scheduler/src/test/resources/mysql_tables.sql). `null` is handled as 0, so no need to update existing records.

### Version 3.3
* Customizable serlizer (PR https://github.com/kagkarlsson/db-scheduler/pull/32)

### Version 3.2
* Customizable table-name for persistence

### Version 3.1
* Future executions can now be fetched using the `scheduler.getScheduledExecutions(..)`

### Version 3.0
* New builders for task-creation, making it clearer what the config-options are. (See `Tasks` class and examples)
* Better default for failure handling for one-time tasks
* Enables recurring tasks to have data
* `Schedule.getNextExecutionTime` can now use all data from `ExecutionComplete`

**Upgrading to 3.x**
* No schema changes
* Task creation are preferrably done through builders in `Tasks` class

### Version 2.0
* Possible to `cancel` and `reschedule` executions.
* Optional data can be stored with the execution. Default using Java Serialization.
* Exposing the `Execution`to the `ExecutionHandler`.

**Upgrading to 2.x**
* Add column `task_data` to the database schema. See table definitions for [postgresql](db-scheduler/src/test/resources/postgresql_tables.sql), [oracle](db-scheduler/src/test/resources/oracle_tables.sql) or [mysql](db-scheduler/src/test/resources/mysql_tables.sql).

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
