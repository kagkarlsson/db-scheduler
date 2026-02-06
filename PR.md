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

- [ ] Add nullable `state` column (text/varchar). (null considered 'active')
- [ ] Suggest appropriate indices in schema-files

### Execution states

| State           | Description                                                                                                                                      |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| (null) / ACTIVE | Active, scheduled for execution (default, backward compatible)                                                                                   |
| COMPLETE        | Execution completed successfully and is kept for deduplication. Can be deleted by automatic process after configured time.                      |
| PAUSED          | Execution has been paused, typically via manual action. Will not execute until resumed.                                                          |
| RECORD          | Execution completed and is kept indefinitely as a historic record. Similar to liquibase migration records - ensures something runs only once.   |
| FAILED          | Execution is marked as permanently failed by its FailureHandlers. Must be manually triggered to retry.                                          |
| WAITING         | Execution is waiting for activation to run.                                                                                                      |

### Query changes

- All existing queries fetching due executions is updated with `(state=ACTIVE or state is null)` condition

### SchedulerClient

- [ ] Let `client.getScheduledExecutions(...)` also return inactive executions (I know, unfortunate
  naming...).
  Not by default, but opt-in via filter
- [ ] New methods on `ScheduledExecution`:
  - `.getState()`
- [ ] New/modified filter methods
    - `.withIncludeInactive()`
    - `ScheduledExecutionsFilter.active()`
    - `ScheduledExecutionsFilter.inactive()`
    - Deprecate `ScheduledExecutionsFilter.all()` (`active` new default)
- [ ] Use `client.reactivate(TaskInstanceId, Instant)` to reschedule deactivated executions. Will reset
  lastSuccess, lastFailure and consecutiveFailures
  - RecurringTasks will not have option to reschedule according to Schedule
- [ ] Use `client.deactivate(TaskInstanceId, State)` to manually deactivate a scheduled execution. Will
  keep lastSuccess, lastFailure and consecutiveFailures
- [ ] Use `client.deleteDeactivated(DeleteOptions)`. Options:
  - olderThan: Instant
  - limit: Int

### Jobs

- [x] Introduce housekeeping-job that regularly deletes deactivated executions. Default 14d for all
except `State.RECORD` which is kept indefinately.

### CompletionHandlers (changes)

- [x] `.onCompleteDeactivate(State)` - deactivate with given state
- [x] `.onCompleteKeepRecord()` - convenience for `.onCompleteDeactivate(State.RECORD)`

### FailureHandlers (changes)

- [ ] MaxRetriesThenRemove(Int, FailureHandler, callback)
- [ ] MaxRetriesThenDeactivate(Int, FailureHandler, State, callback)
- [ ] deprecate MaxRetriesFailureHandler since ambigious

## Backward compatibility / other things

- Only active executions should be considered by dead execution detection
- `state = null` means active/scheduled (existing rows should work without migration)
- `state` column is required (migration needed for existing schemas)
- Consider adding future column `state_details (TEXT)` / `failure_details (TEXT)` to hold exceptions
  from failure
