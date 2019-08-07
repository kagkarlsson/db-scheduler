# Spring Boot Example

This Maven module provides a working example of the [db-scheduler](https://github.com/kagkarlsson/db-scheduler) running in a Spring Boot application using the provided Spring Boot starter.

## Prerequisites

- An existing Spring Boot application
- A working `DataSource` with schema initialized. (In the example HSQLDB is used and schema is automatically applied.)

## How to use

1. Add the following Maven dependency
    ```xml
    <dependency>
        <groupId>com.github.kagkarlsson</groupId>
        <artifactId>db-scheduler-spring-boot-starter</artifactId>
        <version>5.3</version> <!-- Look up the current version -->
    </dependency>
    ```
   *NOTE*: This includes the db-scheduler dependency itself.
2. In your configuration, expose your `Task`'s as Spring beans. If they are recurring, they will automatically be picked up and started.
3. (Optional) Define a bean of type `DbSchedulerCustomizer` if you want to supply a custom naming strategy, serializer or `ExecutorService`.

## Configuration options

The Spring Boot integration exposes some configuration properties. For more configurability, consider defining a bean of type `DbSchedulerCustomzier`.

```
# Listing with default values

db-scheduler.enabled=true
db-scheduler.heartbeat-interval=5m
db-scheduler.polling-interval=30s
db-scheduler.polling-limit=
db-scheduler.table-name=scheduled_tasks
db-scheduler.immediate-execution-enabled=false
db-scheduler.scheduler-name=
db-scheduler.threads=10
```
