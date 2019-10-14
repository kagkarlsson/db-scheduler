package com.github.kagkarlsson.scheduler.boot.config.startup;

import com.github.kagkarlsson.scheduler.Scheduler;
import javax.annotation.PostConstruct;

public class ImmediateStart extends AbstractSchedulerStarter {
  public ImmediateStart(final Scheduler scheduler) {
    super(scheduler);
  }

  @PostConstruct
  void startImmediately() {
    doStart();
  }
}
