package com.github.kagkarlsson.scheduler;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.stream.Stream;

public class StopSchedulerExtension implements AfterEachCallback {

    private Scheduler[] scheduler = new Scheduler[]{};

    public void register(Scheduler ... scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        Stream.of(scheduler).forEach(s -> s.stop(Duration.ofMillis(100)));
    }
}
