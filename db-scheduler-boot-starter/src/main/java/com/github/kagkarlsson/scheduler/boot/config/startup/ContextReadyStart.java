package com.github.kagkarlsson.scheduler.boot.config.startup;

import com.github.kagkarlsson.scheduler.Scheduler;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

public class ContextReadyStart extends AbstractSchedulerStarter {
  public ContextReadyStart(Scheduler scheduler) {
    super(scheduler);
  }

  @EventListener(ContextRefreshedEvent.class)
  public void whenContextIsReady() {
    doStart();
  }
}
