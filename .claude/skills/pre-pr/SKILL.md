---
name: pre-pr
description: Use before opening a pull request against db-scheduler. Runs /finalize, a fresh-context self-review focused on correctness and readability, a red/green check of the tests, and produces the "Assisted by" line for the PR description.
---

# Pre-PR self-review

Follow this before opening a PR, so the maintainer reviews a change that is already in
good shape. If your tool doesn't support skills, follow the steps manually.

## 1. Finalize

Run `/finalize` (format, license headers, `mvn verify`). Stop and fix if it fails.

## 2. Self-review in a fresh context

Review the full diff against `master` using the most capable model available, in a
**fresh context**, not the session that wrote the code. In Claude Code:
`/code-review high` on an Opus-level model.

Review priorities, in order:

1. **Correctness on realistic paths**, including multi-node and concurrent execution
   (optimistic/pessimistic locking, the `version` guard). This is the core of the library.
2. **Readability and a minimal diff.**

Dismiss speculative edge cases unless the fix is trivial and in scope. When a finding can
be fixed by removing code instead of adding it, prefer that.

Also check for the patterns [AI_POLICY.md](../../../AI_POLICY.md) warns about:
single-use abstractions, duplicated or framework-testing tests, boilerplate, comments that
restate the code, and reformatting or renames outside the change.

Fix or consciously dismiss each finding.

## 3. Verify the tests

- **Red/green:** temporarily revert the fix (keep the test) and confirm the new test fails,
  then restore the fix.
- **SQL or `JdbcCustomization` changes:** add a test to `CompatibilityTest` (under
  `db-scheduler/src/test/java/.../compatibility/`) so it runs against every dialect. Run the
  affected dialects, e.g. `mvn test -Dtest=PostgresqlCompatibilityTest`, or all of them with
  `mvn -Ptests-for-ci test`.
- **Public API:** changes should be additive (new overloads rather than changed signatures).
- Update README, examples, or `UPGRADING.md` if behavior or API changed.

## 4. AI line for the PR description

End the PR description with one line naming the model and tool, adding the self-review
only if one was run, e.g.:

```
Assisted by claude-opus-5-5 (Claude Code), self-reviewed with /code-review high.
```

Add a "Verified by:" note only if something beyond the tests was used to verify the change
(e.g. ran it against a real database, manual testing of a Spring Boot app).
