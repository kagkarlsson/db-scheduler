package com.github.kagkarlsson.examples.boot;

import com.github.kagkarlsson.scheduler.SchedulerClient;
import org.slf4j.Logger;
import org.springframework.transaction.support.TransactionTemplate;

public class ExampleContext {
    public SchedulerClient schedulerClient;
    public TransactionTemplate tx;
    private Logger logger;

    public ExampleContext(SchedulerClient schedulerClient, TransactionTemplate tx, Logger logger) {
        this.schedulerClient = schedulerClient;
        this.tx = tx;
        this.logger = logger;
    }

    public void log(String message) {
        logger.info(message);
    }
}
