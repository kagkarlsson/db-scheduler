# PR Review 2: Remaining Points

All bugs and most quality/nit issues from PR-review.md have been fixed. The following points remain.

Issues are categorized as **Bug**, **Design**, **Quality**, or **Nit**.

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
