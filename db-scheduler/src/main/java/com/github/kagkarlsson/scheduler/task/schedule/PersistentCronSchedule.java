/**
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task.schedule;

import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import java.time.ZoneId;

public class PersistentCronSchedule implements ScheduleAndData {
  private static final long serialVersionUID = 1L;
  private final String cronPattern;
  private final String zoneId;
  private final Object data;

  public PersistentCronSchedule(String cronPattern) {
    this(cronPattern, ZoneId.systemDefault(), null);
  }

  public PersistentCronSchedule(String cronPattern, Object data) {
    this(cronPattern, ZoneId.systemDefault(), data);
  }

  public PersistentCronSchedule(String cronPattern, ZoneId zoneId) {
    this(cronPattern, zoneId, null);
  }

  public PersistentCronSchedule(String cronPattern, ZoneId zoneId, Object data) {
    this.cronPattern = cronPattern;
    this.zoneId = zoneId.getId();
    this.data = data;
  }

  @Override
  public String toString() {
    return "PersistentCronSchedule pattern=" + cronPattern;
  }

  @Override
  public Schedule getSchedule() {
    return new CronSchedule(cronPattern, ZoneId.of(zoneId));
  }

  @Override
  public Object getData() {
    return data;
  }
}
