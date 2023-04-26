package com.github.kagkarlsson.scheduler;

import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class StopSchedulerExtension implements AfterEachCallback {

  private Duration waitBeforeInterrupt = Duration.ZERO;
  private Scheduler[] scheduler = new Scheduler[] {};

  public void register(Scheduler... scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    Stream.of(scheduler).forEach(s -> s.stop(waitBeforeInterrupt, Duration.ofMillis(100)));
  }

  public void setWaitBeforeInterrupt(Duration waitBeforeInterrupt) {
    this.waitBeforeInterrupt = waitBeforeInterrupt;
  }
}
