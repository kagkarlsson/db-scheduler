# PR Review 2: Remaining Points

All bugs and most quality/nit issues from PR-review.md have been fixed. The following points remain.

Issues are categorized as **Bug**, **Design**, **Quality**, or **Nit**.

---

## Bug

### 3. Duplicate `serialVersionUID` across two exception classes

**`TaskInstanceNotActiveException.java:20`** and **`TaskInstanceNotDeactivatedException.java:20`** both use `serialVersionUID = 1L`. The original value was changed but both classes were given the same new value. They still need unique values.

---

## Design

### 8. `reschedule(Execution, RescheduleUpdate)` return type is misleading

**`JdbcTaskRepository.java:587`** -- Returns `boolean` but always returns `true` (throws on failure via `updateSingle`). The return type should be `void`, or the method should return `false` on 0 updated rows instead of throwing. Currently callers cannot distinguish success from "always true".

### 11. Repeated `picked(false)/pickedBy(null)/lastHeartbeat(null)` pattern

**`JdbcTaskRepository.java`** -- `deactivate` (line 518–521), `reactivate` (line 542–547), and `reschedule` (lines 590–592) all repeat the same reset of `picked(false)`, `pickedBy(null)`, `lastHeartbeat(null)`. Consider extracting a helper like `ExecutionUpdate.resetPickedState()` to reduce duplication.

---

## Test Coverage Gaps

### 15. Error paths for `client.deactivate()` and `client.reactivate()` are untested

No tests for:
- `TaskInstanceNotFoundException` (deactivating non-existent task)
- `TaskInstanceNotActiveException` (deactivating already-deactivated task)
- `TaskInstanceNotDeactivatedException` (reactivating active task)
- `TaskInstanceCurrentlyExecutingException` (deactivating/reactivating picked task)

### 16. `removeOldDeactivatedExecutions` not tested in CompatibilityTest

The DB-specific SQL for batch deletion is not verified against real databases.

### 17. Dead execution detection active-only filtering not directly tested

The SQL change adding `(state is null or state = 'ACTIVE')` to the dead execution query has no targeted test.

### 18. `State.RECORD` surviving housekeeping is untested

No test verifying that `RECORD` state executions are excluded from the deletion job.

### 19. `MaxRetriesBuilder` without retry strategy throws `IllegalStateException` -- untested

---

## Nits

### 20. Typo in `State.java:40`

`"permanantly"` → `"permanently"` (in the `FAILED` enum constant javadoc).

### 21. Missing `state_execution_time_idx` index in two schemas

`hsql_tables.sql` and `postgresql_custom_tablename.sql` are both missing the `state_execution_time_idx` index that all other DB schemas include.
