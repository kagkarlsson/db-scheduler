package com.github.kagkarlsson.examples.helpers;

import com.github.kagkarlsson.scheduler.Scheduler;

import static org.slf4j.LoggerFactory.getLogger;

public class ExampleHelpers {

    public static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void registerShutdownHook(Scheduler scheduler) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            getLogger(ExampleHelpers.class).info("Received shutdown signal.");
            scheduler.stop();
        }));

    }
}
