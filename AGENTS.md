# db-scheduler

Java library for persistent task scheduling.

## Build & Test

```bash
mvn test                    # Run all tests
mvn test -Dtest=ClassName   # Run specific test class
mvn spotless:apply          # Format code
mvn license:format          # Update license headers
```

## Finalizing PRs

- Run `mvn spotless:apply` and `mvn license:format` before committing
- Keep only essential comments in code — avoid verbose explanations

## Code Style

- Follow existing patterns in the codebase
- Use jspecify for null-safety in new code (`@NullMarked` on package/class, `@Nullable` for nullable fields/params)
- Tests: prefer AssertJ over Hamcrest
- Tests: prefer compact tests — multiple assertions and steps per test is fine to reduce boilerplate
