package com.github.kagkarlsson.examples.boot;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Service;

@Service
public class CounterService {
    private final AtomicLong count = new AtomicLong(0L);

    public void increase() {
        count.incrementAndGet();
    }

    public long read() {
        return count.get();
    }
}
