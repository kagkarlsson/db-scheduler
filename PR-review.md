# PR Review: Deactivation Support

## Summary

Large, well-structured PR implementing execution deactivation (state management) for db-scheduler. The `ExecutionUpdate` builder pattern is a clear improvement over the old index-tracking approach. New tests cover the major happy paths well. The PR aligns with the spec in PR.md on all major features.

Issues below are categorized as **Bug**, **Design**, **Quality**, or **Nit**.

---

## Bugs

### 1. `ScheduledExecution.getState()` returns nullable field instead of null-safe method
**`ScheduledExecution.java:86`** -- Returns `execution.state` (the raw field, which is null for pre-migration rows) instead of `execution.getState()` (which maps null to `State.ACTIVE`). This means callers like `ScheduleRecurringOnStartup` will see `null` for existing rows without the `state` column populated, and `null != State.ACTIVE` evaluates to `true` in Java -- **preventing recurring tasks from being scheduled on startup for pre-migration databases**.

```java
// Current (broken):
public State getState() { return execution.state; }

// Should be:
public State getState() { return execution.getState(); }
```

### 2. Log message uses wrong variable after batch deletion
**`Scheduler.java:470`** -- Logs `removed` (last batch count, which may be 0 when the loop exits on `removed < limit`) instead of `totalRemoved`.

```java
LOG.info("Removed {} old deactivated executions", removed);     // wrong
LOG.info("Removed {} old deactivated executions", totalRemoved); // correct
```

### 3. Duplicate `serialVersionUID` across two exception classes
**`TaskInstanceNotActiveException.java:20`** and **`TaskInstanceNotDeactivatedException.java:20`** both use `serialVersionUID = 3847291650283948172L`. These are different classes and should have unique values.

### 4. `DeactivatedCondition` SQL missing parentheses
**`JdbcTaskRepository.java:971`** -- `"state is not null AND state <> 'ACTIVE'"` is not parenthesized, unlike `ScheduledCondition` which uses `"(state is null OR state = 'ACTIVE')"`. When composed with other conditions via `AND`, the missing parens could cause incorrect evaluation. Should be `"(state is not null AND state <> 'ACTIVE')"`.

---

## Design Issues

### 5. JDBC types leaked into public task API
**`ExecutionOperations.java:39`** -- `deactivate(DeactivationUpdate)` exposes `DeactivationUpdate` (a JDBC-layer record) in the public task API. Similarly `RescheduleUpdate` leaks through `reschedule(RescheduleUpdate)`. These should be abstracted or the update types should live at the task layer, not the jdbc package.

### 6. Naming inconsistency: `lastFailed` vs `lastFailure`
**`DeactivationUpdate.java`** uses `lastFailed` while `RescheduleUpdate`, `ExecutionUpdate`, and the DB column (`last_failure`) all use `lastFailure`. Should be `lastFailure` everywhere.

### 7. SQL Server `removeOldDeactivatedExecutions` deletes without limit
**`JdbcTaskRepository.java:~795-808`** -- When `supportsDeleteWithLimit()` is false (SQL Server), the fallback ignores the `limit` parameter and deletes all matching rows in one statement. Could cause lock escalation and long-running transactions. Should use `DELETE TOP(?) FROM ...` for SQL Server.

### 8. `reschedule(Execution, RescheduleUpdate)` return type is misleading
**`JdbcTaskRepository.java:584`** -- Returns `boolean` but always returns `true` (throws on failure via `updateSingle`). Should be `void`, or should return `false` on 0 updated rows instead of throwing.

### 9. `ScheduledExecutionsFilter` boolean interaction is ambiguous
`includeDeactivated` and `onlyDeactivated` as independent booleans have unclear semantics when both are set. Consider an enum (`ACTIVE_ONLY`, `DEACTIVATED_ONLY`, `ALL`) for clarity.

---

## Quality / Maintenance

### 10. Hardcoded `'ACTIVE'` string in 7+ SQL locations
The literal `'ACTIVE'` appears in `DefaultJdbcCustomization`, `MssqlJdbcCustomization`, `PostgreSqlJdbcCustomization`, `Queries`, `JdbcTaskRepository` (multiple places). Extract a shared constant to reduce maintenance risk.

### 11. Repeated `picked(false)/pickedBy(null)/lastHeartbeat(null)` pattern
**`JdbcTaskRepository.java`** -- The `deactivate`, `reactivate`, and `reschedule` methods all repeat this same triple. Extract a helper like `ExecutionUpdate.resetPickedState()`.

### 12. `DeactivationUpdate` missing `@NullMarked`
`RescheduleUpdate` and `ExecutionUpdate` both have `@NullMarked`, but `DeactivationUpdate` does not.

### 13. Inconsistent `NewValue.of()` vs `new NewValue<>()`
Both forms are used across the codebase. Pick one and be consistent.

### 14. `JdbcConfig` visibility mismatch
`JdbcConfig` is package-private but `ExecutionUpdate.updateSingle(JdbcConfig)` is public. External code cannot call it despite the public modifier.

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
"permanantly" -> "permanently"

### 21. HSQL test schema missing `state_execution_time_idx` index
Every other DB schema includes this index. HSQL schema should match for documentation consistency.

### 22. `OnCompleteDeactivate.state` field should be `final`
**`CompletionHandler.java`** -- Mutable field in what should be an immutable handler.

### 23. `SchedulerTester.getDeactivatedExecutions()` is defined but never called
Dead code in the test helper.

---

## PR.md Checklist Cross-Reference

| Spec Item | Status | Notes |
|-----------|--------|-------|
| `state` column added | Done | All schemas updated |
| Indices suggested | Mostly | Missing in HSQL, postgresql_custom_tablename |
| Due queries filter by state | Done | `(state is null OR state = 'ACTIVE')` |
| `ScheduledExecution.getState()` | **Bug** | Returns null for pre-migration rows (#1) |
| `ScheduledExecutionsFilter` methods | Done | |
| `client.reactivate()` | Done | |
| `client.deactivate()` | Done | |
| `client.deleteDeactivated()` | Done | |
| Housekeeping job | Done | |
| `CompletionHandler.onCompleteDeactivate` | Done | |
| `CompletionHandler.onCompleteKeepRecord` | Done | |
| `FailureHandler.maxRetries` builder | Done | |
| Deprecate `MaxRetriesFailureHandler` | Done | |
| Recurring startup handles deactivated | Done | But depends on bug #1 being fixed |
| Dead execution detection active-only | Done | Query updated, but untested |
