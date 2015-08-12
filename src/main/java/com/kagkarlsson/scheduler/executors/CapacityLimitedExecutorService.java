package com.kagkarlsson.scheduler.executors;

import java.util.concurrent.ExecutorService;

public interface CapacityLimitedExecutorService extends ExecutorService {
	boolean hasFreeExecutor();
}
