# Upgrading

Version-specific upgrade notes. For the full list of changes per release, see
[releases](https://github.com/kagkarlsson/db-scheduler/releases).

**Upgrading to 17.x**
* The Spring Boot starters no longer register a no-op `DbSchedulerCustomizer` bean. Injecting
`DbSchedulerCustomizer` directly will fail to resolve. Configuration previously done by implementing that
interface is now done with a `DbSchedulerOverrides` bean, see
[configuration options](./README.md#configuration-options). An existing `DbSchedulerCustomizer` bean is still
honoured, but it is deprecated and will be removed in a future version.
* `Jackson3Serializer` has moved from the Spring Boot 4 starter to the core module, from
`com.github.kagkarlsson.scheduler.boot.autoconfigure` to `com.github.kagkarlsson.scheduler.serializer`. Its
`Instant` serializers moved from `...boot.autoconfigure.jackson3` to `...serializer.jackson3`. Jackson 3 is an
optional dependency of the core module, so the serializer can now be used without Spring.
* The internal JDBC helpers (`JdbcRunner`, `SQLRuntimeException`, etc.) have moved from
`com.github.kagkarlsson.shaded.jdbc` to `com.github.kagkarlsson.jdbc`. Unlikely to affect anyone, but if you
reference these classes directly you will have to update your imports.
* The starter jars now declare an `Automatic-Module-Name`: `db.scheduler.spring.boot.starter` for the Boot 3
starter and `com.github.kagkarlsson.scheduler.boot` for the Boot 4 starter. If you reference a starter from a
`module-info.java`, replace the previous filename-derived name.
* Logging of long-running executions is enabled by default. An execution still running after
`longRunningExecutionsLoggingThreshold` (default 8 hours) is logged once at `WARN`, including a stack trace for
the executing thread. Turn it off with `logLongRunningExecutions(false)`, or
`db-scheduler.log-long-running-executions=false` for the starter.
* `SchedulerBuilder.pollingInterval(..)` now rejects `null` and durations of zero or less.
* The public constructors of `CurrentlyExecuting` and `ExecutionOperations` take one additional argument each,
and the protected `Scheduler` constructor takes two. Only relevant if you construct these yourself or subclass
`Scheduler`.

**Upgrading to 16.x**
* Java 17+ is required now, since we migrated our codebase to Java 17

**Upgrading to 15.x**
* Priority is a new opt-in feature. To be able to use it, column `priority` and index `priority_execution_time_idx`
must be added to the database schema. See table definitions for
[postgresql](./db-scheduler/src/test/resources/postgresql_tables.sql),
[oracle](./db-scheduler/src/test/resources/oracle_tables.sql) or
[mysql](./db-scheduler/src/test/resources/mysql_tables.sql).
At some point, this column will be made mandatory. This will be made clear in future release/upgrade-notes.

**Upgrading to 8.x**
* Custom Schedules must implement a method `boolean isDeterministic()` to indicate whether they will always produce the same instants or not.

**Upgrading to 4.x**
* Add column `consecutive_failures` to the database schema. See table definitions for [postgresql](./db-scheduler/src/test/resources/postgresql_tables.sql), [oracle](./db-scheduler/src/test/resources/oracle_tables.sql) or [mysql](./db-scheduler/src/test/resources/mysql_tables.sql). `null` is handled as 0, so no need to update existing records.

**Upgrading to 3.x**
* No schema changes
* Task creation are preferably done through builders in `Tasks` class

**Upgrading to 2.x**
* Add column `task_data` to the database schema. See table definitions for [postgresql](./db-scheduler/src/test/resources/postgresql_tables.sql), [oracle](./db-scheduler/src/test/resources/oracle_tables.sql) or [mysql](./db-scheduler/src/test/resources/mysql_tables.sql).
