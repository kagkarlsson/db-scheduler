package com.github.kagkarlsson.scheduler.boot.config;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.github.kagkarlsson.scheduler.JdbcTaskRepository;
import java.time.Duration;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("db-scheduler")
public class DbSchedulerProperties {
    /***
     * <p>Number of threads.
     */
    private int threads = 10;

    /**
     * How often to update the heartbeat timestamp for running executions.
     */
    @DurationUnit(MINUTES)
    @NotNull
    private Duration heartbeatInterval = Duration.ofMinutes(5);

    /**
     * <p>Name of this scheduler-instance. The name is stored in the database when an execution is
     * picked by a scheduler.
     *
     * <p>If the name is {@code null} or not configured, the hostname of the running machine will be
     * used.
     */
    private String schedulerName;

    /**
     * <p>Name of the table used to track task-executions. Must match the database. Change name in the
     * table definitions accordingly when creating or modifying the table.
     */
    @NotNull
    private String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;

    /**
     * <p>If this is enabled, the scheduler will attempt to directly execute tasks that are scheduled
     * to {@code now()}, or a time in the past. For this to work, the call to {@code schedule(..)}
     * must not occur from within a transaction, because the record will not yet be visible to the
     * scheduler (if this is a requirement, see the method
     * {@code scheduler.triggerCheckForDueExecutions())}
     */
    private boolean immediateExecution = false;

    /**
     * <p>How often the scheduler checks the database for due executions.
     */
    @DurationUnit(SECONDS)
    @NotNull
    private Duration pollingInterval = Duration.ofSeconds(30);

    /**
     * <p>Maximum number of executions to fetch on a check for due executions.
     */
    private Optional<Integer> pollingLimit = Optional.empty();

    public int getThreads() {
        return threads;
    }

    public void setThreads(final int threads) {
        this.threads = threads;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(final Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public String getSchedulerName() {
        return schedulerName;
    }

    public void setSchedulerName(String schedulerName) {
        this.schedulerName = schedulerName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    public boolean isImmediateExecution() {
        return immediateExecution;
    }

    public void setImmediateExecution(boolean immediateExecution) {
        this.immediateExecution = immediateExecution;
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }

    public void setPollingInterval(final Duration pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public Optional<Integer> getPollingLimit() {
        return pollingLimit;
    }

    public void setPollingLimit(final Optional<Integer> pollingLimit) {
        this.pollingLimit = pollingLimit;
    }
}
