# db-scheduler Project Guide for Claude

## Project Overview

**db-scheduler** is a Java library that provides a persistent, cluster-friendly task scheduler as an alternative to Quartz. It's designed to be simpler, embeddable, and high-performance, using a single database table for persistence.

### Key Features
- **Cluster-friendly**: Guarantees execution by single scheduler instance
- **Persistent tasks**: Uses single `scheduled_tasks` database table
- **Embeddable**: Built to be embedded in existing applications
- **High throughput**: Handles 2k-10k executions/second
- **Minimal dependencies**: Only requires slf4j

## Project Structure

### Multi-Module Maven Project
```
db-scheduler/                    # Root project (parent POM)
├── db-scheduler/               # Core library module
├── db-scheduler-boot-starter/  # Spring Boot starter
├── examples/                   # Example implementations
│   ├── features/              # Plain Java examples
│   └── spring-boot-example/   # Spring Boot example
└── test/benchmark/            # Performance benchmarking
```

### Core Architecture Components

1. **Scheduler**: Main entry point, manages execution lifecycle
2. **Executor**: Thread pool management with priority support
3. **TaskRepository**: Database persistence layer (JDBC-based)
4. **SchedulerBuilder**: Fluent configuration API
5. **Task Types**:
   - `RecurringTask`: Scheduled according to a schedule
   - `OneTimeTask`: Single execution
   - `CustomTask`: Fully customizable behavior

### Key Packages
- `com.github.kagkarlsson.scheduler`: Core scheduler components
- `com.github.kagkarlsson.scheduler.task`: Task definitions and handlers
- `com.github.kagkarlsson.scheduler.jdbc`: Database customizations
- `com.github.kagkarlsson.scheduler.serializer`: Data serialization (Java/Gson/Jackson)
- `com.github.kagkarlsson.scheduler.stats`: Metrics integration (Micrometer)

## Build and Development Setup

### Prerequisites
- **Java 11+** (target runtime, but tests run on newer versions)
- **Maven 3.8+**
- **Docker** (for database compatibility tests)

### Maven Commands

#### Build and Test
```bash
# Basic build and test (excludes compatibility tests)
mvn clean package

# Skip all tests
mvn clean package -DskipTests=true

# Run tests including compatibility tests (requires Docker)
mvn clean test -Ptests-for-ci

# Run ALL tests including cluster tests (slow, requires Docker)
mvn clean test -Pall-tests

# Apply code formatting
mvn spotless:apply

# Check license headers
mvn license:check
```

#### Test Profiles
- **Default**: Excludes `compatibility` and `compatibility-cluster` tags
- **`tests-for-ci`**: Excludes only `compatibility-cluster` (GitHub Actions)
- **`all-tests`**: Runs ALL tests including cluster tests

### Database Support
- **PostgreSQL** (primary, best performance)
- **MySQL/MariaDB** (requires `alwaysPersistTimestampInUTC`)
- **SQL Server (MSSQL)**
- **Oracle**
- **HSQLDB** (testing only)

## Code Quality and Standards

### Formatting
- **Google Java Format** via Spotless plugin
- **EditorConfig** for consistent formatting
- Auto-format: `mvn spotless:apply`

### Testing Strategy
- **JUnit 5** for unit tests
- **TestContainers** for database integration tests
- **Embedded databases** for fast tests (HSQLDB)
- **Hamcrest** matchers for assertions

### License
- **Apache License 2.0**
- License headers required on all `.java` files
- Check: `mvn license:check`

## Key Configuration Patterns

### Basic Scheduler Setup
```java
Scheduler scheduler = Scheduler
    .create(dataSource)
    .startTasks(recurringTask)
    .threads(10)
    .pollingInterval(Duration.ofSeconds(10))
    .enablePriority()  // For priority-based execution
    .build();

scheduler.start();
```

### Priority Pools (Recent Feature)
```java
Scheduler scheduler = Scheduler.create(dataSource, tasks)
    .threads(5)                    // Default pool
    .enablePriority()              // Enable priority support
    .addWorkerPool(3, Priority.HIGH) // High-priority pool
    .addWorkerPool(2, Priority.MEDIUM) // Medium-priority pool
    .build();
```

### Spring Boot Configuration
Located in `application.properties`:
```properties
db-scheduler.threads=10
db-scheduler.polling-interval=10s
db-scheduler.priority-enabled=false
db-scheduler.immediate-execution-enabled=false
```

## Important Development Notes

### Current Git Context
- **Branch**: `priority-pools` (feature branch)
- **Modified File**: `Executor.java` (priority pools implementation)
- **Recent Features**: Priority-based worker pools (#702)

### Database Schema
- Single table: `scheduled_tasks`
- **Priority column**: Added in v15.x (opt-in feature)
- Schema files in `db-scheduler/src/test/resources/`

### Performance Considerations
- **Lock-and-fetch strategy**: Better for high throughput (PostgreSQL/SQL Server/MySQL 8+)
- **Fetch-and-lock-on-execute**: Default, works with all databases
- **Priority pools**: Ensure high-priority tasks get immediate execution

### Testing Guidelines
- Fast tests: Use HSQLDB embedded database
- Integration tests: Use TestContainers
- Mark slow tests with `@Tag("compatibility")` or `@Tag("compatibility-cluster")`

## Common Development Tasks

### Adding New Features
1. Start with tests in `db-scheduler/src/test/java/`
2. Core implementation in `db-scheduler/src/main/java/`
3. Update `SchedulerBuilder` for configuration
4. Add Spring Boot support in `db-scheduler-boot-starter/`
5. Create examples in `examples/` modules

### Working with Serialization
- Default: Java serialization
- Optional: Gson, Jackson serializers
- Custom: Implement `Serializer` interface
- Migration: Use `SerializerWithFallbackDeserializers`

### Database Compatibility
- Test with multiple databases using TestContainers
- Database-specific customizations in `jdbc/` package
- Auto-detection via `AutodetectJdbcCustomization`

## Dependencies

### Core (Minimal)
- `slf4j-api` (logging)
- `cron-utils` (shaded, for cron expressions)

### Optional
- `micrometer-core` (metrics)
- `gson` or `jackson` (JSON serialization)

### Test Dependencies
- JUnit 5, Hamcrest, Mockito
- TestContainers for database testing
- Various database drivers (PostgreSQL, MySQL, SQL Server, Oracle)

## Useful Resources

- **Main README**: Comprehensive usage guide and examples
- **CONTRIBUTING.md**: Contribution guidelines
- **Examples**: `examples/` directory with working code
- **Spring Boot Example**: Complete web application example

This guide provides the essential context for working effectively with the db-scheduler codebase, understanding its architecture, and following the established development patterns.

# Current notes - IMPORTANT

- Priority pools are not merged yet; do not worry about backwards compatiblity when doing changes (unless the changes affect prior logic)
