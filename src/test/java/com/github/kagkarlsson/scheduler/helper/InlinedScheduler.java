package com.github.kagkarlsson.scheduler.helper;

import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class InlinedScheduler extends Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(InlinedScheduler.class);
    private final SettableClock clock;

    InlinedScheduler(SettableClock clock, TaskRepository taskRepository, TaskResolver taskResolver, int maxThreads, ExecutorService executorService, SchedulerName schedulerName, Waiter waiter, Duration heartbeatInterval, boolean executeImmediately, StatsRegistry statsRegistry, List<OnStartup> onStartup) {
        super(clock, taskRepository, taskResolver, maxThreads, executorService, schedulerName, waiter, heartbeatInterval, executeImmediately, statsRegistry, onStartup);
        this.clock = clock;
    }

    public SettableClock getClock() {
        return clock;
    }

    public void tick(Duration moveClockForward) {
        clock.set(clock.now.plus(moveClockForward));
    }

    public void setTime(Instant newtime) {
        clock.set(newtime);
    }

    public void runAnyDueExecutions() {
        super.executeDue();
    }

    public void runDeadExecutionDetection() {
        super.detectDeadExecutions();
    }


    public void start() {
        LOG.info("Starting inlined scheduler. Executing on-startup tasks.");
        executeOnStartup();
    }

}
