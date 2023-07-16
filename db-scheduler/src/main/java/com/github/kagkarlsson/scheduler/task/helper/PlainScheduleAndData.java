/*
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
package com.github.kagkarlsson.scheduler.task.helper;

import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import java.io.Serializable;
import java.util.Objects;

public class PlainScheduleAndData implements ScheduleAndData, Serializable {
  private static final long serialVersionUID = 1L;
  private final Schedule schedule;
  private final Object data;

  private PlainScheduleAndData() { // For serializers
    schedule = null;
    data = null;
  }

  public PlainScheduleAndData(Schedule schedule) {
    this.schedule = schedule;
    this.data = null;
  }

  public PlainScheduleAndData(Schedule schedule, Object data) {
    this.schedule = schedule;
    this.data = data;
  }

  @Override
  public Schedule getSchedule() {
    return schedule;
  }

  @Override
  public Object getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    com.github.kagkarlsson.scheduler.task.helper.PlainScheduleAndData that =
        (com.github.kagkarlsson.scheduler.task.helper.PlainScheduleAndData) o;
    return Objects.equals(schedule, that.schedule) && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schedule, data);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" + "schedule=" + schedule + ", data=" + data + '}';
  }
}
