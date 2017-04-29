# db-scheduler

Persistent scheduler for future execution of tasks, recurring or ad-hoc.

Inspired by the need for a clustered `java.util.concurrent.ScheduledExecutorService` simpler than Quartz.

## Features

* **Cluster-friendly**. Guarantees execution by single scheduler instance.
* **Persistent** tasks. Requires single database-table for persistence.
* **Simple**.
* **Minimal dependencies**. (slf4j)

## Getting started

1. Add maven dependency
```
<dependency>
    <groupId>com.github.kagkarlsson</groupId>
  	<artifactId>db-scheduler</artifactId>
  	<version>1.9</version>
</dependency>
```

2. Create the `scheduled_tasks` table in your schema. See table definition for [postgresql](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/postgresql_tables.sql), [oracle](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/oracle_tables.sql) or [mysql](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/mysql_tables.sql).

3. Instantiate and start the scheduler.

```java
final MyHourlyTask hourlyTask = new MyHourlyTask();

final Scheduler scheduler = Scheduler
        .create(dataSource)
        .startTasks(hourlyTask)
        .threads(5)
        .build();

scheduler.start();
```
## How it works

A single database table is used to track future task-executions. When a task-execution is due, db-scheduler picks it and executes it. When the execution is done, the `Task` is consulted to see what should be done. For example, a `RecurringTask` is typically rescheduled in the future based on its `Schedule`.

Optimistic locking is used to guarantee that a single scheduler instance gets to pick a task-execution.


#### Recurring tasks

The term _recurring task_ is used for tasks that should be run regularly, according to some schedule (see `RecurringTask`).

When the execution of a recurring task has finished, a `Schedule` is consulted to determine what the next time for execution should be, and a future task-execution is created for that time (i.e. the task-instance is _rescheduled_). The time chosen will be the nearest in time according to the `Schedule`, but still in the future.

To create the initial execution for a `RecurringTask`, the scheduler has a method  `startTasks(...)` that takes a list of tasks that should be "started" if they do not already have a future execution. Note: The first execution-time will not be according to the schedule, but simply `now()`.

#### Ad-hoc tasks

The other type of task has been named _ad-hoc task_, but is most typically something that should be run once at a certain time in the future, a `OneTimeTask`.

In addition to encode some data into the `instanceId`of a task-execution, it is possible to store arbitrary binary data in a separate field for use at execution-time.

#### Reliability - handling crashes

To avoid missed task-executions, the scheduler also regularly checks for _dead_ executions. Should one be found, the `Task`is consulted to see what should be done. A dead `RecurringTask` is typically rescheduled to `now()`.

#### Things to note / gotchas

* There are no guarantees that all instants in a schedule for a `RecurringTask` will be executed. The `Schedule` is consulted after the previous task-execution finishes, and the closest time in the future will be selected for next execution-time. A new type of task may be added in the future to provide such functionality.

* The methods on `SchedulerClient` (`scheduleForExecution` etc) will run using a new `Connection`from the `DataSource`provided. To have the action be a part of a transaction, something like Spring's `TransactionAwareDataSourceProxy` may be used.

## More examples

#### Simple task definition

Less verbose task-definitions using `ComposableTask`.

```java
final RecurringTask myHourlyTask = ComposableTask.recurringTask(
    "my-hourly-task",
    FixedDelay.of(ofHours(1)),
    () -> System.out.println("Executed!"));

final Scheduler scheduler = Scheduler
        .create(dataSource)
        .startTasks(myHourlyTask)
        .threads(5)
        .build();

scheduler.start();
```

#### Recurring tasks

Start the recurring task on start-up. Upon completion, `hourlyTask` will be re-scheduled according to the defined schedule.

```java
final MyHourlyTask hourlyTask = new MyHourlyTask();

final Scheduler scheduler = Scheduler
        .create(dataSource)
        .startTasks(hourlyTask)
        .threads(5)
        .build();

// hourlyTask is automatically scheduled on startup if not already started (i.e. in the db)
scheduler.start();
```

Custom task class for a recurring task.

```java
public static class MyHourlyTask extends RecurringTask {

  public MyHourlyTask() {
    super("my-hourly-task", FixedDelay.of(Duration.ofHours(1)));
  }

  @Override
  public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
    System.out.println("Executed!");
  }
}
```



#### Ad-hoc tasks

Schedule the ad-hoc task for execution at a certain time in the future. The instance-id may be used to encode metadata (e.g. an id), since the instance-id will be available for the execution-handler.

```java
final MyAdhocTask myAdhocTask = new MyAdhocTask();

final Scheduler scheduler = Scheduler
        .create(dataSource, myAdhocTask)
        .threads(5)
        .build();

scheduler.start();

// Schedule the task for execution a certain time in the future
scheduler.scheduleForExecution(Instant.now().plusSeconds(5), myAdhocTask.instance("1045"));
```

Custom task class for an ad-hoc task.

```java
public static class MyAdhocTask extends OneTimeTask {

  public MyAdhocTask() {
    super("my-adhoc-task");
  }

  @Override
  public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
    System.out.println("Executed!");
  }
}
```


#### Register shutdown-hook for graceful shutdown

```java
RecurringTask myRecurringTask = new MyHourlyTask();
Task myAdhocTask = new MyAdhocTask();

final Scheduler scheduler = Scheduler
        .create(dataSource, myAdhocTask)
        .startTasks(myRecurringTask)
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
