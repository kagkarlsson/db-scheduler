package com.github.kagkarlsson.scheduler.task.schedule;

public enum CronStyle {
  CRON4J,
  QUARTZ,
  UNIX,
  SPRING,
  SPRING53;

  private CronStyle() {}
}
