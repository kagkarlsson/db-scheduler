package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.ComposableTask;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static java.time.Duration.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

public class SchedulerPerfTest {

    private TestTasks.CountingHandler<Void> handler;
    private SettableClock clock;

    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();
    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
        handler = new TestTasks.CountingHandler<>(Duration.ofSeconds(1));
    }

    private Scheduler schedulerFor(ExecutorService executor, int threadPoolSize, int batchcount, Task<?> ... tasks) {
        final StatsRegistry statsRegistry = StatsRegistry.NOOP;
        TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, Arrays.asList(tasks));
        JdbcTaskRepository taskRepository = new JdbcTaskRepository(postgres.getDataSource(), false, DEFAULT_TABLE_NAME, taskResolver, new SchedulerName.Fixed("scheduler1"), clock);

        final Scheduler scheduler = new Scheduler(clock, taskRepository, taskRepository, taskResolver, threadPoolSize, executor,
            new SchedulerName.Fixed("name"), new Waiter(ZERO), ofSeconds(1), false,
            statsRegistry, PollingStrategyConfig.DEFAULT_FETCH, ofDays(14), Duration.ofSeconds(batchcount), LogLevel.DEBUG, true, new ArrayList<>());
        //stopScheduler.register(scheduler);
        return scheduler;
    }

    @Test
    public void PerfTest() throws InterruptedException {
        int threadPoolSize = 300;
        int batch = 10;
        int upperlimit = 3;   // 3 is the upperlimit of the each fetch
        OneTimeTask<Void> oneTimeTask = TestTasks.oneTime("OneTime", Void.class, handler);
        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(threadPoolSize, ExecutorUtils.defaultThreadFactoryWithPrefix("PerfTest-"));
        Scheduler scheduler = schedulerFor(executor, threadPoolSize, batch, oneTimeTask);

        Instant executionTime = clock.now().plus(ofMinutes(1));
        int taskid = 0;
        int totalTasks = batch*upperlimit*threadPoolSize;
        while( ++taskid <= totalTasks ) {
            scheduler.schedule(oneTimeTask.instance(String.valueOf(taskid)), executionTime);
        }

        clock.set(executionTime);
        System.out.println("perftest started ... " + LocalTime.now());
        int lastCount = -1;
        while( handler.timesExecuted.get() != lastCount) {
            lastCount = handler.timesExecuted.get();
            scheduler.executeDue();
        }

        long taskCount = 0;
        do {
            Thread.sleep(1000);
            taskCount = executor.getTaskCount();
            System.out.println("workqueue " + taskCount);
        } while( taskCount > 0 );

        scheduler.stop(Duration.ofSeconds(batch), Duration.ofSeconds(2*batch));
        System.out.println(lastCount + " perftest finished ... " + LocalTime.now());
        // Since execution is executed in an async way, we need to wait for a while to let the execution finish before asserting
        //Thread.sleep(batch*1000);
        assertThat(handler.timesExecuted.get(), is(totalTasks));
    }
}
