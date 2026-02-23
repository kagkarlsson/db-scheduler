# Deactivation Support - GitHub Issue #27

Add functionality to put executions in a deactivated state, stopping further executions,
but keeping it as a historic record for deduplicatation or possibility to resume.

## High-level

- `state` column for holding the state of the execution.
- Only active executions are picked and run.
- Non-active state = "deactivated"
- CompletionHandlers and FailureHandlers now get the option to deactivate the execution and set to a
  deactivated state (in addition to the previous options: remove and reschedule)

## Components

### Schema/query changes

- [ ] Add nullable `state` column (text/varchar). (null considered 'active')
- [ ] Suggest appropriate indices in schema-files
- [ ] Existing queries for fetching due is updated with state-condition: `(state=ACTIVE or state is null)`

### Execution states

| State           | Description                                                                                                                                      |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| (null) / ACTIVE | Active, scheduled for execution (default, backward compatible)                                                                                   |
| COMPLETE        | Execution completed successfully and is kept for deduplication. Can be deleted by automatic process after configured time.                      |
| PAUSED          | Execution has been paused, typically via manual action. Will not execute until resumed.                                                          |
| RECORD          | Execution completed and is kept indefinitely as a historic record. Similar to liquibase migration records - ensures something runs only once.   |
| FAILED          | Execution is marked as permanently failed by its FailureHandlers. Must be manually triggered to retry.                                          |
| WAITING         | Execution is waiting for activation to run.                                                                                                      |


### SchedulerClient

- [x] Let `client.getScheduledExecutions(...)` also return deactivated executions.
  Not by default, but opt-in via filter
- [x] New methods on `ScheduledExecution`:
  - `.getState()`
- [x] New/modified filter methods
    - `.withIncludeDeactivated()`
    - `ScheduledExecutionsFilter.active()`
    - `ScheduledExecutionsFilter.deactivated()`
    - Deprecate `ScheduledExecutionsFilter.all()` (`active` new default)
- [x] Use `client.reactivate(TaskInstanceId, Instant)` to reschedule deactivated executions. Will reset
  lastSuccess, lastFailure and consecutiveFailures
  - RecurringTasks will not have option to reschedule according to Schedule
- [x] Use `client.deactivate(TaskInstanceId, State)` to manually deactivate a scheduled execution. Will
  keep lastSuccess, lastFailure and consecutiveFailures
- [x] Use `client.deleteDeactivated(DeleteOptions)`. Options:
  - olderThan: Instant
  - limit: Int

### Jobs

- [x] Introduce housekeeping-job that regularly deletes deactivated executions. Default 14d for all
except `State.RECORD` which is kept indefinately.

### CompletionHandlers (changes)

- [x] `.onCompleteDeactivate(State)` - deactivate with given state
- [x] `.onCompleteKeepRecord()` - convenience for `.onCompleteDeactivate(State.RECORD)`

### FailureHandlers (changes)

- [x] `FailureHandler.maxRetries(n).retryEvery(duration).thenRemove(callback)` - builder API
- [x] `FailureHandler.maxRetries(n).withBackoff(duration).thenDeactivate(state, callback)` - builder API
- [x] `FailureHandler.maxRetries(n).retryEvery(duration).then((complete, ops) -> ...)` - full control callback
- [x] deprecate MaxRetriesFailureHandler since ambiguous

### OnStartup

- [x] Recurring tasks that have been deactivated should not be reactivated when scheduler restarts.
  Logs a WARN for now if Recurring is paused.

## Other things

- Only active executions should be considered by dead execution detection
- `state = null` means active/scheduled (existing rows should work without migration)
- `state` column will be required (migration needed for existing schemas)
- Consider adding future column `state_details (TEXT)` / `failure_details (TEXT)` to hold exceptions
  from failure

### Possible minor breaking changes

- reschedule throw `TaskInstanceException` instead of `ExecutionException`

### Future improvements

- Maven plugin for verifying nullability (e.g. NullAway)
