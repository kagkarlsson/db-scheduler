/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.boot.config;

import com.github.kagkarlsson.scheduler.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.PollingStrategyConfig;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import java.time.Duration;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.validation.annotation.Validated;

import static java.time.temporal.ChronoUnit.*;

@Validated
@ConfigurationProperties("db-scheduler")
public class DbSchedulerProperties {
    /**
     * Whether to enable auto configuration of the db-scheduler.
     */
    private boolean enabled = true;

    /***
     * <p>Number of threads.
     */
    private int threads = 10;

    /**
     * How often to update the heartbeat timestamp for running executions.
     */
    @DurationUnit(MINUTES)
    @NotNull
    private Duration heartbeatInterval = SchedulerBuilder.DEFAULT_HEARTBEAT_INTERVAL;

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
    private boolean immediateExecutionEnabled = false;

    /**
     * <p>How often the scheduler checks the database for due executions.
     */
    @DurationUnit(SECONDS)
    @NotNull
    private Duration pollingInterval = SchedulerBuilder.DEFAULT_POLLING_INTERVAL;

    /**
     * <p> What polling-strategy to use. Valid values are: FETCH,LOCK_AND_FETCH
     */
    private PollingStrategyConfig.Type pollingStrategy = SchedulerBuilder.DEFAULT_POLLING_STRATEGY.type;
    /**
     * <p> The limit at which more executions are fetched from the database after fetching a full batch.</p>
     */
    private double pollingStrategyLowerLimitFractionOfThreads = SchedulerBuilder.DEFAULT_POLLING_STRATEGY.lowerLimitFractionOfThreads;
    /**
     * <p> For Type=FETCH, the number of due executions fetched from the database in each batch.</p>
     * <p> For Type=LOCK_AND_FETCH, the maximum number of executions to pick and queue for execution.</p>
     */
    private double pollingStrategyUpperLimitFractionOfThreads = SchedulerBuilder.DEFAULT_POLLING_STRATEGY.upperLimitFractionOfThreads;

    /**
     * <p>Whether to start the scheduler when the application context has been loaded or as soon as
     * possible.
     */
    private boolean delayStartupUntilContextReady = false;

    /**
     * <p>The time after which executions with unknown tasks are automatically deleted.</p>
     */
    @DurationUnit(HOURS)
    @NotNull
    private Duration deleteUnresolvedAfter = SchedulerBuilder.DEFAULT_DELETION_OF_UNRESOLVED_TASKS_DURATION;


    /**
     * How long the scheduler will wait before interrupting executor-service threads.
     * If you find yourself using this, consider if it is possible to instead regularly check
     * <code>executionContext.getSchedulerState().isShuttingDown()</code> in the ExecutionHandler and abort long-running task.
     */
    @DurationUnit(SECONDS)
    @NotNull
    private Duration shutdownMaxWait = SchedulerBuilder.SHUTDOWN_MAX_WAIT;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

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

    public boolean isImmediateExecutionEnabled() {
        return immediateExecutionEnabled;
    }

    public void setImmediateExecutionEnabled(boolean immediateExecutionEnabled) {
        this.immediateExecutionEnabled = immediateExecutionEnabled;
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }

    public void setPollingInterval(final Duration pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public boolean isDelayStartupUntilContextReady() {
        return delayStartupUntilContextReady;
    }

    public void setDelayStartupUntilContextReady(final boolean delayStartupUntilContextReady) {
        this.delayStartupUntilContextReady = delayStartupUntilContextReady;
    }

    public Duration getDeleteUnresolvedAfter() {
        return deleteUnresolvedAfter;
    }

    public void setDeleteUnresolvedAfter(Duration deleteUnresolvedAfter) {
        this.deleteUnresolvedAfter = deleteUnresolvedAfter;
    }

    public Duration getShutdownMaxWait() {
        return shutdownMaxWait;
    }

    public void setShutdownMaxWait(Duration shutdownMaxWait) {
        this.shutdownMaxWait = shutdownMaxWait;
    }

    public PollingStrategyConfig.Type getPollingStrategy() {
        return pollingStrategy;
    }

    public void setPollingStrategy(PollingStrategyConfig.Type pollingStrategy) {
        this.pollingStrategy = pollingStrategy;
    }

    public double getPollingStrategyLowerLimitFractionOfThreads() {
        return pollingStrategyLowerLimitFractionOfThreads;
    }

    public void setPollingStrategyLowerLimitFractionOfThreads(double pollingStrategyLowerLimitFractionOfThreads) {
        this.pollingStrategyLowerLimitFractionOfThreads = pollingStrategyLowerLimitFractionOfThreads;
    }

    public double getPollingStrategyUpperLimitFractionOfThreads() {
        return pollingStrategyUpperLimitFractionOfThreads;
    }

    public void setPollingStrategyUpperLimitFractionOfThreads(double pollingStrategyUpperLimitFractionOfThreads) {
        this.pollingStrategyUpperLimitFractionOfThreads = pollingStrategyUpperLimitFractionOfThreads;
    }
}
