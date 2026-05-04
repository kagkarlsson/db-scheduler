---

description: Format, license-header, and verify before committing.
------------------------------------------------------------------

Run the pre-commit gauntlet for db-scheduler in this order, stopping on the first
failure and reporting it:

1. `mvn verify` — runs tests, license check, dependency analysis, spotless check.
2. `mvn license:format` — adds Apache license headers to any new `src/main` files.
3. `mvn spotless:apply` — formats Java, pom.xml, and markdown.

If `mvn verify` fails on dependency-plugin warnings, fix the unused/undeclared
dependency rather than disabling the check. Do not pass `-DskipChecks` unless explicitly
asked — those checks are what `/finalize` exists to enforce.

After everything passes, report `git status` so the user can review what spotless or
license:format may have touched.
