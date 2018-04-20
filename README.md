# db-scheduler

![build status](https://travis-ci.org/kagkarlsson/db-scheduler.svg?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.kagkarlsson/db-scheduler/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.kagkarlsson/db-scheduler)
[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

Task-scheduler for Java that was inspired by the need for a clustered `java.util.concurrent.ScheduledExecutorService` simpler than Quartz.

## Features

* **Cluster-friendly**. Guarantees execution by single scheduler instance.
* **Persistent** tasks. Requires single database-table for persistence.
* **Embeddable**. Built to be embedded in existing applications.
* **Simple**.
* **Minimal dependencies**. (slf4j)

## Getting started

1. Add maven dependency
```
<dependency>
    <groupId>com.github.kagkarlsson</groupId>
    <artifactId>db-scheduler</artifactId>
    <version>3.0</version>
</dependency>
```

2. Create the `scheduled_tasks` table in your database-schema. See table definition for [postgresql](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/postgresql_tables.sql), [oracle](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/oracle_tables.sql) or [mysql](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/mysql_tables.sql).

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

For more examples, continue reading. For details on the inner workings, see [How it works](#how-it-works).

## Examples

### Recurring task

Define a _recurring_ task and schedule the task's first execution on start-up using the `startTasks` builder-method. Upon completion, the task will be re-scheduled according to the defined schedule.

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


### Ad-hoc tasks / One-time tasks

An instance of an _ad-hoc_ task has a single execution-time some time in the future (i.e. non-recurring). The instance-id must be unique within this task, and may be used to encode some metadata (e.g. an id). For more complex state, custom serializable java objects are supported (as used in the example).

Define a _onetime_ task and start the scheduler:
 
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
| `.pollingInterval(Duration)`  |  30s  | How often the scheduler checks the database for due executions  |
| `.heartbeatInterval(Duration)`  | 5m | How often to update the heartbeat timestamp for running executions  |
| `.schedulerName(SchedulerName)`  | hostname  | Name of this scheduler-instance. The name is stored in the database when an execution is picked by a scheduler. |


### Task configuration

Tasks are created using one of the builder-classes in `Tasks`. The builders have sensible defaults, but the following options can be overridden. 

| Option  | Default | Description |
| ------------- | ---- | ------------- |
| `.onFailure(FailureHandler)`  | see desc.  | What to do when a `ExecutionHandler` throws an exception. By default, _Recurring tasks_ are rescheduled according to their `Schedule` _one-time tasks_ are retried again in 5m. |
| `.onDeadExecution(DeadExecutionHandler)`  | `ReviveDeadExecution`  | What to do when a _dead executions_ is detected, i.e. an execution with a stale heartbeat timestamp. By default dead executions are rescheduled to `now()`. |
| `.initialData(T initialData)`  | `null`  | The data to use the first time a _recurring task_ is scheduled. |



## How it works

A single database table is used to track future task-executions. When a task-execution is due, db-scheduler picks it and executes it. When the execution is done, the `Task` is consulted to see what should be done. For example, a `RecurringTask` is typically rescheduled in the future based on its `Schedule`.

Optimistic locking is used to guarantee that a one and only one scheduler-instance gets to pick a task-execution.


### Recurring tasks

The term _recurring task_ is used for tasks that should be run regularly, according to some schedule (see `RecurringTask`).

When the execution of a recurring task has finished, a `Schedule` is consulted to determine what the next time for execution should be, and a future task-execution is created for that time (i.e. it is _rescheduled_). The time chosen will be the nearest time according to the `Schedule`, but still in the future.

To create the initial execution for a `RecurringTask`, the scheduler has a method  `startTasks(...)` that takes a list of tasks that should be "started" if they do not already have a future execution. Note: The first execution-time will not be according to the schedule, but simply `now()`.

### Ad-hoc tasks

The other type of task has been named _ad-hoc task_, but is most typically something that should be run once at a certain time in the future, a `OneTimeTask`.

In addition to encode some data into the `instanceId`of a task-execution, it is possible to store arbitrary binary data in a separate field for use at execution-time.

### Dead executions

During execution, the scheduler regularly updates a heartbeat-time for the task-execution. If an execution is marked as executing, but is not receiving updates to the heartbeat-time, it will be considered a _dead execution_ after time X. That may for example happen if the JVM running the scheduler suddenly exits.

When a dead execution is found, the `Task`is consulted to see what should be done. A dead `RecurringTask` is typically rescheduled to `now()`.


### Things to note / gotchas

* There are no guarantees that all instants in a schedule for a `RecurringTask` will be executed. The `Schedule` is consulted after the previous task-execution finishes, and the closest time in the future will be selected for next execution-time. A new type of task may be added in the future to provide such functionality.

* The methods on `SchedulerClient` (`schedule`, `cancel`, `reschedule`) and the `CompletionHandler` will run using a new `Connection`from the `DataSource`provided. To have the action be a part of a transaction, it must be taken care of by the `DataSource`provided, for example using something like Spring's `TransactionAwareDataSourceProxy`.

* Currently, the precision of db-scheduler is depending on the `pollingInterval` (default 10s) which specifies how often to look in the table for due executions.

## Versions / upgrading

### Version 3.1
* Future executions can now be fetched using the `scheduler.getScheduledExecutions(..)`

### Version 3.0
* New builders for task-creation, making it clearer what the config-options are. (See `Tasks` class and examples)
* Better default for failure handling for one-time tasks 
* Enables recurring tasks to have data

**Upgrading to 3.x**
* No schema changes
* Task creation are preferrably done through builders in `Tasks` class 

### Version 2.0
* Possible to `cancel` and `reschedule` executions.
* Optional data can be stored with the execution. Default using Java Serialization.
* Exposing the `Execution`to the `ExecutionHandler`.

**Upgrading to 2.x**
* Add column `task_data` to the database schema. See table definitions for [postgresql](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/postgresql_tables.sql), [oracle](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/oracle_tables.sql) or [mysql](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/mysql_tables.sql).

## FAQ

Coming