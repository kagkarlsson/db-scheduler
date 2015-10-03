# db-scheduler

Simple persistent scheduler for scheduled tasks, recurring or ad-hoc.

## Features

* Cluster-friendly, guarantees execution by single scheduler instance.
* Requires only one database-table for persistence.
* Minimal dependencies (slf4j, micro-jdbc)


## Examples
### Recurring tasks

Schedule the initial execution on start-up. If an execution is already scheduled for the task, the existing execution is kept. A RecurringTask will be re-scheduled according to the defined schedule upon completion.

```java
private static void recurringTask(DataSource dataSource) {

    final MyHourlyTask hourlyTask = new MyHourlyTask();

    final Scheduler scheduler = Scheduler
            .create(dataSource, hourlyTask)
            .startTasks(hourlyTask)
            .build();

    // Recurring task is automatically scheduled
    scheduler.start();
}
```

Custom task class for a recurring task.

```java
public static class MyHourlyTask extends RecurringTask {

  public MyHourlyTask() {
    super("task_name", FixedDelay.of(Duration.ofHours(1)));
  }

  @Override
  public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
    System.out.println("Executed!");
  }
}
```



### Ad-hoc tasks

Schedule the task for execution at a certain time in the future. The instance-id may be used to encode metadata (e.g. an id), since the instance-id will be available for the execution-handler. An execution for a OneTimeTask is deleted upon completion.

```java
private static void adhocExecution(Scheduler scheduler, MyAdhocTask myAdhocTask) {

  // Schedule the task for execution a certain time in the future
  scheduler.scheduleForExecution(LocalDateTime.now().plusMinutes(5), myAdhocTask.instance("1045"));
}
```

Custom task class for an ad-hoc task.

```java
public static class MyAdhocTask extends OneTimeTask {

  public MyAdhocTask() {
    super("adhoc_task_name");
  }

  @Override
  public void execute(TaskInstance taskInstance, ExecutionContext executionContext) {
    System.out.println("Executed!");
  }
}
```




### Instantiating and starting the Scheduler

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

// schedule one-time task for execution. recurring task is automatically scheduled
scheduler.scheduleForExecution(now().plusSeconds(20), myAdhocTask.instance("1045"));
```
