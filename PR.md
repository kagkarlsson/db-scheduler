# Deactivation Support - GitHub Issue #27

Add ability to pause or disable tasks (recurring).

## High-level

- `state` column for the state of the execution. Only active executions are run.
- non-active executions are called inactive
- CompletionHandlers and FailureHandlers now get the option to deactivate the execution and set a
  non-active state (in addition to the previous options: remove and reschedule)

## Components of the PR

### Schema changes

Migration (ALTER TABLE):

- Add nullable `state` column (text/varchar). (null considered 'active')
- Suggests appropriate indices in schema-files

### Execution states

| State           | Description                                                                                                            |
|-----------------|------------------------------------------------------------------------------------------------------------------------|
| (null) / ACTIVE | Active, scheduled for execution (default, backward compatible)                                                         |
| RECORD          | Never deleted by automatic processes. Like liquibase migration records - ensures something runs only once, guaranteed. |
| COMPLETE        | Kept for duplication-elimination. Can be deleted by automatic process after configured time.                           |
| FAILED          | Failed its attempts, must be manually retried.                                                                         |
| PAUSED          | Recurring or one-time task temporarily disabled. Will not execute until resumed.                                       |
| WAITING         | Waiting for activation. Not attempted to run yet.                                                                      |

### Query changes

- All existing queries fetching due executions is updated with `(state=ACTIVE or state is null)` condition
  - considering

### SchedulerClient

- Let `client.getScheduledExecutions(...)` also return inactive executions (I know, unfortunate
  naming...).
  Not by default, but opt-in via filter
- New methods on `ScheduledExecution`:
  - `.getState()`
- New/modified filter methods
    - `.withIncludeInactive()`
    - `ScheduledExecutionsFilter.active()`
    - `ScheduledExecutionsFilter.inactive()`
    - Deprecate `ScheduledExecutionsFilter.all()` (`active` new default)
- Use `client.reactivate(TaskInstanceId, Instant)` to reschedule deactivated executions. Will reset
  lastSuccess, lastFailure and consecutiveFailures
  - RecurringTasks will not have option to reschedule according to Schedule
- Use `client.deactivate(TaskInstanceId, State)` to manually deactivate a scheduled execution. Will
  keep lastSuccess, lastFailure and consecutiveFailures
- Use `client.deleteDeactivated(DeleteOptions)`. Options:
  - olderThan: Instant
  - limit: Int

### CompletionHandlers (changes)

* `.onCompleteDeactivate(State.COMPLETE)`
* `.onCompleteKeepRecord()`

### FailureHandlers (changes)

* MaxRetriesThenRemove(Int, FailureHandler, callback)
* MaxRetriesThenDeactivate(Int, FailureHandler, State, callback)
* deprecate MaxRetriesFailureHandler since ambigious

## Backward compatibility / other things

- Only active executions should be considered by dead execution detection
- `state = null` means active/scheduled (existing rows should work without migration)
- `safeGetString()` utility for reading state column (handles missing column in old schemas)
- consider detecting whether `state` column exists or not, and avoid using in queries if not exists
- consider adding future column `state_details (TEXT)` / `failure_details (TEXT)` to hold exceptions
  from failure
