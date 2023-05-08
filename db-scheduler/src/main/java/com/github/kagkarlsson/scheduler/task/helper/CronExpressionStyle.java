package com.github.kagkarlsson.scheduler.task.helper;

public enum CronExpressionStyle {
  CRON4J,
  QUARTZ,
  UNIX,
  SPRING,
  SPRING53;

  private CronExpressionStyle() {}
}
