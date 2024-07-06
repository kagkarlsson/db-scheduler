package com.github.kagkarlsson.scheduler.event;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.ExecutionHandler;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
class ExecutionChainTest {

  @Test
  public void happy() {
    AtomicInteger counter = new AtomicInteger(0);
    CountingExecutionHandler executionHandler = new CountingExecutionHandler(counter);
    ExecutionChain chain =
        new ExecutionChain(
            List.of(new CountingInterceptor(counter), new CountingInterceptor(counter)),
            executionHandler);

    chain.proceed(new TaskInstance("task1", "id1"), dummyContext());

    assertEquals(3, counter.get());
  }

  private static ExecutionContext dummyContext() {
    return new ExecutionContext(null, null, null, null);
  }

  private static class CountingInterceptor implements ExecutionInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(CountingInterceptor.class);
    private final AtomicInteger counter;

    public CountingInterceptor(AtomicInteger counter) {
      this.counter = counter;
    }

    @Override
    public CompletionHandler<?> execute(
        TaskInstance<?> taskInstance, ExecutionContext executionContext, ExecutionChain chain) {

      counter.incrementAndGet();
      return chain.proceed(taskInstance, executionContext);
    }
  }

  private static class CountingExecutionHandler implements ExecutionHandler<Void> {

    private final AtomicInteger counter;

    public CountingExecutionHandler(AtomicInteger counter) {
      this.counter = counter;
    }

    @Override
    public CompletionHandler<Void> execute(
        TaskInstance<Void> taskInstance, ExecutionContext executionContext) {
      counter.incrementAndGet();
      return null;
    }
  }
}
