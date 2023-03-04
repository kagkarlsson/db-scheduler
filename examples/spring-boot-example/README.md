# Spring Boot Example

This Maven module provides a working example of the [db-scheduler](https://github.com/kagkarlsson/db-scheduler) running in a Spring Boot application using the provided Spring Boot starter.


**Task-names for the examples**

* sample-one-time-task
* chained-step-1
* long-running-task
* multi-instance-recurring-task
* parallel-job-spawner
* state-tracking-recurring-task
* transactionally-staged-task


## Running examples

Examples need to be started, currently by sending a request to the web-server:

```shell
curl -X POST http://localhost:8080/admin/start -H "Content-Type: application/json" -d '{"taskName":"sample-one-time-task"}'
```

Replace `taskName` with one of the examples from above.

