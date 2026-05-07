---
name: schema-change
description: Use when changing the db-scheduler database schema (adding/removing/altering columns, indexes, or the scheduled_tasks table). Walks through the lock-step update across every supported DB dialect, the matching JdbcCustomization changes, and the documentation copies. Skip for code-only changes that don't touch table structure.
---

# Schema change checklist

The library supports six DB dialects and ships DDL for each. A schema change that
misses one dialect ships a broken release. Treat this as a single atomic change.

## 1. DDL files (test resources)

Update **all six** in lock-step. Path: `db-scheduler/src/test/resources/`

- `hsql_tables.sql`
- `postgresql_tables.sql`
- `mysql_tables.sql`
- `mariadb_tables.sql`
- `mssql_tables.sql`
- `oracle_tables.sql`

Read all six first; column ordering and types differ per dialect (e.g. `TIMESTAMP WITH
TIME ZONE` vs `DATETIME2` vs `TIMESTAMP(6) WITH TIME ZONE`). Don't blindly copy from
one dialect to another — preserve each dialect's existing type conventions.

There's also `postgresql_custom_tablename.sql` under
`src/test/resources/com/github/kagkarlsson/scheduler/` — check whether your change
needs to be reflected there too (it tests configurable table name; usually yes if
columns change).

## 2. JdbcCustomization classes

Path: `db-scheduler/src/main/java/com/github/kagkarlsson/scheduler/jdbc/`

If your change affects how rows are read/written (new column, type change, nullable→
not-null, etc.), update each relevant class:

- `DefaultJdbcCustomization.java` — base behavior
- `PostgreSqlJdbcCustomization.java`
- `OracleJdbcCustomization.java`
- `MssqlJdbcCustomization.java`
- `MySQLJdbcCustomization.java`
- `MySQL8JdbcCustomization.java`
- `MariaDBJdbcCustomization.java`
- `AutodetectJdbcCustomization.java` — only if dialect-detection logic changes
- `JdbcCustomization.java` (interface) — if the contract itself changes

Also check `JdbcTaskRepository.java` and `QueryBuilder.java` for SQL that references
the changed columns.

## 3. Documentation DDL copies

The README and/or `docs/` may carry copies of the DDL for users. Grep for a column
name from the table to find them:

```bash
grep -rn "scheduled_tasks" --include="*.md" .
```

Update any matching SQL blocks so users get the current schema.

## 4. Verify

```bash
mvn -pl db-scheduler test                                # PostgreSQL tests
```

Dialects other than PostgreSQL currently need to be tested on CI (need to run on amd64 arch).

## 5. Migration note for users

Schema changes are user-visible. Add a brief migration note to the changelog / release
notes describing the required `ALTER TABLE` (per dialect if they differ).

## 6. Finalize

Run `/finalize` before committing.
