# db-scheduler

Persistent scheduler for future execution of tasks, recurring or ad-hoc.

Inspired by the need for a clustered `java.util.concurrent.ScheduledExecutorService` simpler than Quartz.

## Features

* **Simple**.
* **Cluster-friendly**. Guarantees execution by single scheduler instance.
* **Persistent** tasks. Requires single database-table for persistence.
* **Minimal dependencies** (slf4j)

## Getting started

1. Add maven dependency
```
<dependency>
    <groupId>com.github.kagkarlsson</groupId>
  	<artifactId>db-scheduler</artifactId>
  	<version>1.2</version>
</dependency>
```

2. Create the `scheduled_tasks` table in your schema. See table definition for [postgresql](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/postgresql_tables.sql) and [oracle](https://github.com/kagkarlsson/db-scheduler/blob/master/src/test/resources/oracle_tables.sql).

3. Instantiate and start the scheduler.

```java
final MyHourlyTask hourlyTask = new MyHourlyTask();

final Scheduler scheduler = Scheduler
    .create(dataSource, hourlyTask)
    .startTasks(hourlyTask)
    .threads(5)
    .build();

scheduler.start();
```

## More examples
### Recurring tasks

Start the recurring task on start-up. Upon completion, `hourlyTask` will be re-scheduled according to the defined schedule.

```java
final MyHourlyTask hourlyTask = new MyHourlyTask();

final Scheduler scheduler = Scheduler
    .create(dataSource, hourlyTask)
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



### Ad-hoc tasks

Schedule the ad-hoc task for execution at a certain time in the future. The instance-id may be used to encode metadata (e.g. an id), since the instance-id will be available for the execution-handler.

```java
final MyAdhocTask myAdhocTask = new MyAdhocTask();

final Scheduler scheduler = Scheduler
    .create(dataSource, myAdhocTask)
    .threads(5)
    .build();

scheduler.start();

// Schedule the task for execution a certain time in the future
scheduler.scheduleForExecution(LocalDateTime.now().plusMinutes(5), myAdhocTask.instance("1045"));
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

### Register shutdown-hook for graceful shutdown

```java
RecurringTask myRecurringTask = new MyHourlyTask();
Task myAdhocTask = new MyAdhocTask();

final Scheduler scheduler = Scheduler
    .create(dataSource, myAdhocTask, myRecurringTask )
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
