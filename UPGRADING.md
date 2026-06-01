# Upgrading

Version-specific upgrade notes. For the full list of changes per release, see
[releases](https://github.com/kagkarlsson/db-scheduler/releases).

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
