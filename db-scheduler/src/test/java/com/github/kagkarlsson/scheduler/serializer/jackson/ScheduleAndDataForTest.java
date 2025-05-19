package com.github.kagkarlsson.scheduler.serializer.jackson;

import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import java.io.Serial;

public class ScheduleAndDataForTest implements ScheduleAndData {
  @Serial
  private static final long serialVersionUID = 1L; // recommended when using Java serialization

  private final CronSchedule schedule;
  private final Long id;

  private ScheduleAndDataForTest() {
    this(null, null);
  }

  public ScheduleAndDataForTest(CronSchedule schedule, Long id) {
    this.schedule = schedule;
    this.id = id;
  }

  @Override
  public CronSchedule getSchedule() {
    return schedule;
  }

  @Override
  public Long getData() {
    return id;
  }
}
