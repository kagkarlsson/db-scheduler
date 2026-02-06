package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import java.io.Serial;

public class ScheduleAndDataForTest implements ScheduleAndData {
  @Serial
  private static final long serialVersionUID = 1L; // recommended when using Java serialization

  private final CronSchedule schedule;
  private final Long data;

  private ScheduleAndDataForTest() {
    this(null, null);
  }

  public ScheduleAndDataForTest(CronSchedule schedule, Long data) {
    this.schedule = schedule;
    this.data = data;
  }

  @Override
  public CronSchedule getSchedule() {
    return schedule;
  }

  @Override
  public Long getData() {
    return data;
  }
}
