package com.kagkarlsson.scheduler.executors;

import com.google.common.util.concurrent.ForwardingExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

public class LimitedThreadPool extends ForwardingExecutorService implements CapacityLimitedExecutorService {

	private final int capacity;
	private final ThreadPoolExecutor delegate;

	public LimitedThreadPool(int capacity, ThreadFactory threadFactory) {
		this.capacity = capacity;
		this.delegate = new ThreadPoolExecutor(
				capacity, capacity,
				1, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(capacity), // should not really matter
				threadFactory,
				new ThreadPoolExecutor.AbortPolicy());
	}

	@Override
	protected ExecutorService delegate() {
		return delegate;
	}

	@Override
	public boolean hasFreeExecutor() {
		return delegate.getActiveCount() < capacity;
	}
}
