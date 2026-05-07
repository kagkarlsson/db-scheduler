# db-scheduler

Java library for persistent task scheduling on a relational database. Uses either optimistic locking
(`UPDATE SET ..., version=version+1 WHERE version=<version>) or pessimistic locking (
`SELECT ... FOR UPDATE`, or dialect equivalents) to coordinate execution across nodes,
so correctness across DB dialects is the central concern of the codebase.

## Modules

- `db-scheduler/` — core library, no Spring dependency.
- `db-scheduler-spring-boot-starter-parent/`
  - `db-scheduler-spring-common/` — shared Spring code
  - `db-scheduler-spring-boot3-starter/` — Spring Boot 3.x starter
  - `db-scheduler-spring-boot4-starter/` — Spring Boot 4.x starter
- `examples/` — runnable examples for documentational purposes (`features/`, `spring-boot-example/`); not published.

Core package: `com.github.kagkarlsson.scheduler`. Key entry points: `Scheduler`,
`SchedulerBuilder`, `SchedulerClient`, `TaskRepository`, `task/*`, `jdbc/*` (dialect
handling), `serializer/*`.

`com.cronutils` are shaded into the published jar
under `com.github.kagkarlsson.shaded.*`. Don't expose these on the public API.

## Build & test

JDK 17, Docker required (compatibility tests).

```bash
mvn test                       # Unit + PostgreSQL-backed tests. Fast.
mvn verify                     # Adds license check, dependency analysis, spotless.
mvn -DskipChecks verify        # Skip license/header/dep-analysis enforcement.
mvn test -Dtest=ClassName[#method]
```

## Code style

- `mvn spotless:apply` — google-java-format (GOOGLE), pom sortPom, markdown flexmark.
- `mvn license:format` — adds Apache headers; build fails on missing headers in
  `src/main` (test sources excluded). Source: `.license/license-header.txt`.
- introducing jspecify for null-safety: prefer `@NullMarked` at package level, `@Nullable` on
  individual references.
- AssertJ over Hamcrest in tests.
- Compact tests are fine — multiple assertions/steps per test to reduce boilerplate.
- Comments only when WHY is non-obvious.

## Finalizing changes

Before committing run `/finalize` (or manually `mvn spotless:apply && mvn license:format
&& mvn verify`). For schema/DDL changes, use the `schema-change` skill — DDL must stay
in lock-step across all six dialect files.

Per CONTRIBUTING.md: open an issue before non-trivial PRs; one focused change per PR.

## Easy to get wrong

- Forgetting one dialect's DDL or matching `JdbcCustomization` when changing the
  schema. Use the `schema-change` skill.
- Referencing shaded packages (`com.github.kagkarlsson.shaded.*`) from the public API.
- Adding a dependency without scope: `maven-dependency-plugin` runs with
  `failOnWarning=true`, so unused/undeclared deps break the build.

