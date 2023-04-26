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
package com.github.kagkarlsson.scheduler;

import static com.github.kagkarlsson.scheduler.ExecutorUtils.defaultThreadFactoryWithPrefix;
import static com.github.kagkarlsson.scheduler.Scheduler.THREAD_PREFIX;
import static java.util.Optional.ofNullable;

import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerBuilder.class);
    public static final double UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH = 3.0;

    public static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(10);
    public static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofMinutes(5);
    public static final Duration DEFAULT_DELETION_OF_UNRESOLVED_TASKS_DURATION = Duration.ofDays(14);
    public static final Duration SHUTDOWN_MAX_WAIT = Duration.ofMinutes(30);
    public static final PollingStrategyConfig DEFAULT_POLLING_STRATEGY = new PollingStrategyConfig(
            PollingStrategyConfig.Type.FETCH, 0.5, UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH);
    public static final LogLevel DEFAULT_FAILURE_LOG_LEVEL = LogLevel.WARN;
    public static final boolean LOG_STACK_TRACE_ON_FAILURE = true;

    protected Clock clock = new SystemClock(); // if this is set, waiter-clocks must be updated

    protected final DataSource dataSource;
    protected SchedulerName schedulerName;
    protected int executorThreads = 10;
    protected final List<Task<?>> knownTasks = new ArrayList<>();
    protected final List<OnStartup> startTasks = new ArrayList<>();
    protected Waiter waiter = new Waiter(DEFAULT_POLLING_INTERVAL, clock);
    protected StatsRegistry statsRegistry = StatsRegistry.NOOP;
    protected Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    protected Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
    protected String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;
    protected boolean enableImmediateExecution = false;
    protected ExecutorService executorService;
    protected Duration deleteUnresolvedAfter = DEFAULT_DELETION_OF_UNRESOLVED_TASKS_DURATION;
    protected JdbcCustomization jdbcCustomization = null;
    protected Duration shutdownMaxWait = SHUTDOWN_MAX_WAIT;
    protected boolean commitWhenAutocommitDisabled = false;
    protected PollingStrategyConfig pollingStrategyConfig = DEFAULT_POLLING_STRATEGY;
    protected LogLevel logLevel = DEFAULT_FAILURE_LOG_LEVEL;
    protected boolean logStackTrace = LOG_STACK_TRACE_ON_FAILURE;
    private boolean registerShutdownHook = false;

    public SchedulerBuilder(DataSource dataSource, List<Task<?>> knownTasks) {
        this.dataSource = dataSource;
        this.knownTasks.addAll(knownTasks);
    }

    @SafeVarargs
    public final <T extends Task<?> & OnStartup> SchedulerBuilder startTasks(T... startTasks) {
        return startTasks(Arrays.asList(startTasks));
    }

    public <T extends Task<?> & OnStartup> SchedulerBuilder startTasks(List<T> startTasks) {
        knownTasks.addAll(startTasks);
        this.startTasks.addAll(startTasks);
        return this;
    }

    public SchedulerBuilder pollingInterval(Duration pollingInterval) {
        waiter = new Waiter(pollingInterval, clock);
        return this;
    }

    public SchedulerBuilder heartbeatInterval(Duration duration) {
        this.heartbeatInterval = duration;
        return this;
    }

    public SchedulerBuilder threads(int numberOfThreads) {
        this.executorThreads = numberOfThreads;
        return this;
    }

    public SchedulerBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    public SchedulerBuilder statsRegistry(StatsRegistry statsRegistry) {
        this.statsRegistry = statsRegistry;
        return this;
    }

    public SchedulerBuilder schedulerName(SchedulerName schedulerName) {
        this.schedulerName = schedulerName;
        return this;
    }

    public SchedulerBuilder serializer(Serializer serializer) {
        this.serializer = serializer;
        return this;
    }

    public SchedulerBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public SchedulerBuilder enableImmediateExecution() {
        this.enableImmediateExecution = true;
        return this;
    }

    public SchedulerBuilder deleteUnresolvedAfter(Duration deleteAfter) {
        this.deleteUnresolvedAfter = deleteAfter;
        return this;
    }

    public SchedulerBuilder jdbcCustomization(JdbcCustomization jdbcCustomization) {
        this.jdbcCustomization = jdbcCustomization;
        return this;
    }

    public SchedulerBuilder shutdownMaxWait(Duration shutdownMaxWait) {
        this.shutdownMaxWait = shutdownMaxWait;
        return this;
    }

    public SchedulerBuilder commitWhenAutocommitDisabled(boolean commitWhenAutocommitDisabled) {
        this.commitWhenAutocommitDisabled = commitWhenAutocommitDisabled;
        return this;
    }

    public SchedulerBuilder pollUsingFetchAndLockOnExecute(double lowerLimitFractionOfThreads,
            double executionsPerBatchFractionOfThreads) {
        this.pollingStrategyConfig = new PollingStrategyConfig(PollingStrategyConfig.Type.FETCH,
                lowerLimitFractionOfThreads, executionsPerBatchFractionOfThreads);
        return this;
    }

    public SchedulerBuilder pollUsingLockAndFetch(double lowerLimitFractionOfThreads,
            double upperLimitFractionOfThreads) {
        this.pollingStrategyConfig = new PollingStrategyConfig(PollingStrategyConfig.Type.LOCK_AND_FETCH,
                lowerLimitFractionOfThreads, upperLimitFractionOfThreads);
        return this;
    }

    public SchedulerBuilder failureLogging(LogLevel logLevel, boolean logStackTrace) {
        if (logLevel == null) {
            throw new IllegalArgumentException("Log level must not be null");
        }
        this.logLevel = logLevel;
        this.logStackTrace = logStackTrace;
        return this;
    }

    public SchedulerBuilder registerShutdownHook() {
        this.registerShutdownHook = true;
        return this;
    }

    public Scheduler build() {
        if (schedulerName == null) {
            schedulerName = new SchedulerName.Hostname();
        }

        final TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, knownTasks);
        final JdbcCustomization jdbcCustomization = ofNullable(this.jdbcCustomization)
                .orElseGet(() -> new AutodetectJdbcCustomization(dataSource));
        final JdbcTaskRepository schedulerTaskRepository = new JdbcTaskRepository(dataSource, true, jdbcCustomization,
                tableName, taskResolver, schedulerName, serializer, clock);
        final JdbcTaskRepository clientTaskRepository = new JdbcTaskRepository(dataSource, commitWhenAutocommitDisabled,
                jdbcCustomization, tableName, taskResolver, schedulerName, serializer, clock);

        ExecutorService candidateExecutorService = executorService;
        if (candidateExecutorService == null) {
            candidateExecutorService = Executors.newFixedThreadPool(executorThreads,
                    defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-"));
        }

        LOG.info(
                "Creating scheduler with configuration: threads={}, pollInterval={}s, heartbeat={}s enable-immediate-execution={}, table-name={}, name={}",
                executorThreads, waiter.getWaitDuration().getSeconds(), heartbeatInterval.getSeconds(),
                enableImmediateExecution, tableName, schedulerName.getName());

        final Scheduler scheduler = new Scheduler(clock, schedulerTaskRepository, clientTaskRepository, taskResolver,
                executorThreads, candidateExecutorService, schedulerName, waiter, heartbeatInterval,
                enableImmediateExecution, statsRegistry, pollingStrategyConfig, deleteUnresolvedAfter, shutdownMaxWait,
                logLevel, logStackTrace, startTasks);

        if (registerShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Received shutdown signal.");
                scheduler.stop();
            }));
        }

        return scheduler;
    }
}
