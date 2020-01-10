/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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
    private static final int POLLING_CONCURRENCY_MULTIPLIER = 3;

    protected Clock clock = new SystemClock(); // if this is set, waiter-clocks must be updated

    protected final DataSource dataSource;
    protected SchedulerName schedulerName = new SchedulerName.Hostname();
    protected int executorThreads = 10;
    protected final List<Task<?>> knownTasks = new ArrayList<>();
    protected final List<OnStartup> startTasks = new ArrayList<>();
    protected Waiter waiter = new Waiter(Duration.ofSeconds(10), clock);
    protected int pollingLimit;
    protected boolean useDefaultPollingLimit;
    protected StatsRegistry statsRegistry = StatsRegistry.NOOP;
    protected Duration heartbeatInterval = Duration.ofMinutes(5);
    protected Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
    protected String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;
    protected boolean enableImmediateExecution = false;
    protected ExecutorService executorService;

    public SchedulerBuilder(DataSource dataSource, List<Task<?>> knownTasks) {
        this.dataSource = dataSource;
        this.knownTasks.addAll(knownTasks);
        this.pollingLimit = calculatePollingLimit();
        this.useDefaultPollingLimit = true;
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

    public SchedulerBuilder pollingLimit(int pollingLimit) {
        if(pollingLimit <= 0) {
            throw new IllegalArgumentException("pollingLimit must be a positive integer");
        }
        this.pollingLimit = pollingLimit;
        this.useDefaultPollingLimit = false;
        return this;
    }

    private int calculatePollingLimit() {
        return executorThreads * POLLING_CONCURRENCY_MULTIPLIER;
    }

    public SchedulerBuilder heartbeatInterval(Duration duration) {
        this.heartbeatInterval = duration;
        return this;
    }

    public SchedulerBuilder threads(int numberOfThreads) {
        this.executorThreads = numberOfThreads;
        if(useDefaultPollingLimit) {
            this.pollingLimit = calculatePollingLimit();
        }
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

    public Scheduler build() {
        if (pollingLimit < executorThreads) {
            LOG.warn("Polling-limit is less than number of threads. Should be equal or higher.");
        }
        final TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, knownTasks);
        final JdbcTaskRepository taskRepository = new JdbcTaskRepository(dataSource, tableName, taskResolver, schedulerName, serializer);

        ExecutorService candidateExecutorService = executorService;
        if (candidateExecutorService == null) {
            candidateExecutorService = Executors.newFixedThreadPool(executorThreads, defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-"));
        }

        LOG.info("Creating scheduler with configuration: threads={}, pollInterval={}s, heartbeat={}s enable-immediate-execution={}, table-name={}, name={}",
            executorThreads,
            waiter.getWaitDuration().getSeconds(),
            heartbeatInterval.getSeconds(),
            enableImmediateExecution,
            tableName,
            schedulerName.getName());
        return new Scheduler(clock, taskRepository, taskResolver, executorThreads, candidateExecutorService,
                schedulerName, waiter, heartbeatInterval, enableImmediateExecution, statsRegistry, pollingLimit, startTasks);
    }
}
