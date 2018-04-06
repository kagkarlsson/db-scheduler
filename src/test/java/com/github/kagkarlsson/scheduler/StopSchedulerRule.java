package com.github.kagkarlsson.scheduler;

import org.junit.rules.ExternalResource;

import java.util.stream.Stream;

public class StopSchedulerRule extends ExternalResource {

    private Scheduler[] scheduler = new Scheduler[]{};

    @Override
    protected void after() {
        Stream.of(scheduler).forEach(Scheduler::stop);
    }

    public void register(Scheduler ... scheduler) {
        this.scheduler = scheduler;
    }
}
