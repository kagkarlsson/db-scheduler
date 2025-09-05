# db-scheduler Development Guidelines

This document provides essential information for developers working on the db-scheduler project.

## Build/Configuration Instructions

### Prerequisites

- Java 11+ (JDK 11 is the minimum supported version)
- Maven 3.8.0+
- Docker (required for running compatibility tests)

### Building the Project

1. Clone the repository:
   ```
   git clone https://github.com/kagkarlsson/db-scheduler.git
   cd db-scheduler
   ```

2. Build using Maven:
   ```
   mvn clean package
   ```

3. Build without running tests:
   ```
   mvn clean package -DskipTests=true
   ```

### Project Structure

The project consists of three main modules:

- **db-scheduler**: The core library
- **db-scheduler-boot-starter**: Spring Boot starter for db-scheduler
- **examples**: Example applications demonstrating various features

## Testing Information

### Running Tests

1. Run basic tests:
   ```
   mvn clean test
   ```

2. Run compatibility tests (requires Docker):
   ```
   mvn -Ptests-for-ci clean test
   ```

3. Run all tests including compatibility-cluster tests:
   ```
   mvn -Pall-tests clean test
   ```

### Test Configuration

The project uses the following testing frameworks and tools:

- JUnit 5 for test execution
- Hamcrest for assertions
- Mockito for mocking
- TestContainers for database integration tests
- HSQLDB for in-memory database tests

### Writing Tests

#### Test Structure

Tests should follow this general structure:

```java
package com.github.kagkarlsson.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.jupiter.api.Test;

public class MyFeatureTest {

    @Test
    public void should_do_something_when_condition() {
        // Arrange
        // Set up test data and conditions

        // Act
        // Execute the code being tested

        // Assert
        // Verify the results
        assertThat(actual, is(expected));
    }
}
```

#### Testing with Databases

For tests requiring a database, use TestContainers or the embedded HSQLDB:

```java
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class DatabaseTest {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14")
            .withDatabaseName("db-scheduler-test")
            .withUsername("test")
            .withPassword("test");

    // Test methods...
}
```

#### Testing the Scheduler

For testing the scheduler without a real database, use the `ManualScheduler` and `SettableClock` classes:

```java
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;

import java.time.Duration;
import java.time.Instant;

public class SchedulerTest {

    @Test
    public void test_scheduler_execution() {
        // Create a task
        OneTimeTask<Void> task = Tasks.oneTime("my-task", Void.class)
            .execute((instance, ctx) -> {
                // Task execution logic
            });

        // Create a manual scheduler
        ManualScheduler scheduler = TestHelper.createManualScheduler(task);

        // Schedule a task
        scheduler.schedule(task.instance("1"), Instant.now());

        // Execute due tasks
        scheduler.runAnyDueExecutions();

        // Verify execution
        // ...
    }
}
```

## Additional Development Information

### Code Style

The project uses Google Java Format for code formatting:

1. Apply code formatting:
   ```
   mvn spotless:apply
   ```

2. Check code formatting:
   ```
   mvn spotless:check
   ```

The project also uses an `.editorconfig` file to maintain consistent formatting. Ensure your editor respects this configuration.

### Pull Request Guidelines

1. Open an issue before submitting a PR to discuss the changes
2. Focus on one feature/fix per PR
3. Ensure all tests pass locally before submitting
4. Follow the code style guidelines
5. Include appropriate tests for new functionality

### Database Schema

The project requires a database table for storing scheduled tasks. SQL scripts for creating this table are available for different database systems:

- PostgreSQL: `db-scheduler/src/test/resources/postgresql_tables.sql`
- MySQL: `db-scheduler/src/test/resources/mysql_tables.sql`
- MS SQL Server: `db-scheduler/src/test/resources/mssql_tables.sql`
- Oracle: `db-scheduler/src/test/resources/oracle_tables.sql`

### Serialization

By default, the scheduler uses Java serialization for task data. For production use, consider using one of the alternative serializers:

- `GsonSerializer`
- `JacksonSerializer`

Example configuration:

```java
Scheduler scheduler = Scheduler.create(dataSource)
    .serializer(new JacksonSerializer())
    .build();
```

### Logging

The project uses SLF4J for logging. In your application, provide an appropriate SLF4J implementation (e.g., Logback, Log4j).
