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
package com.github.kagkarlsson.scheduler.task.schedule;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Spring-style cron-pattern schedule */
public class CronSchedule implements Schedule, Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private static final String DISABLED = "-";
  private static final Logger LOG = LoggerFactory.getLogger(CronSchedule.class);

  private final String pattern;
  private final ZoneId zoneId;
  private final CronStyle cronStyle;
  private transient ExecutionTime cronExecutionTime; // lazily initialized

  private CronSchedule() { // For serializers
    pattern = null;
    zoneId = ZoneId.systemDefault();
    cronStyle = CronStyle.SPRING53;
  }

  public CronSchedule(String pattern) {
    this(pattern, ZoneId.systemDefault());
  }

  public CronSchedule(String pattern, ZoneId zoneId) {
    this(pattern, zoneId, CronStyle.SPRING53);
  }

  public CronSchedule(String pattern, ZoneId zoneId, CronStyle cronStyle) {
    this.pattern = pattern;
    this.cronStyle = cronStyle != null ? cronStyle : CronStyle.SPRING53;
    if (zoneId == null) {
      throw new IllegalArgumentException("zoneId may not be null");
    }
    this.zoneId = zoneId;
    lazyInitExecutionTime();
  }

  @Override
  public Instant getNextExecutionTime(ExecutionComplete executionComplete) {
    lazyInitExecutionTime(); // for deserialized objects

    // frame the 'last done' time in the context of the time zone for this schedule
    // so that expressions like "0 05 13,20 * * ?" (New York) can operate in the
    // context of the desired time zone
    ZonedDateTime lastDone = ZonedDateTime.ofInstant(executionComplete.getTimeDone(), zoneId);

    Optional<ZonedDateTime> nextTime = cronExecutionTime.nextExecution(lastDone);
    if (nextTime.isEmpty()) {
      LOG.error(
          "Cron-pattern did not return any further execution-times. This behavior is currently "
              + "not supported by the scheduler. Setting next execution-time to far-future, "
              + "aka \"NEVER\" ({})",
          NEVER);

      return Schedule.NEVER;
    }

    return nextTime.get().toInstant();
  }

  private void lazyInitExecutionTime() {
    if (cronExecutionTime != null) {
      return;
    }

    synchronized (this) {
      if (cronExecutionTime == null) {
        if (isDisabled()) {
          cronExecutionTime = new CronSchedule.DisabledScheduleExecutionTime();
        } else {
          CronParser parser =
              new CronParser(
                  CronDefinitionBuilder.instanceDefinitionFor(
                      cronStyle == null
                          ? getCronType(CronStyle.SPRING53)
                          : getCronType(cronStyle)));
          Cron cron = parser.parse(pattern);
          cronExecutionTime = ExecutionTime.forCron(cron);
        }
      }
    }
  }

  private CronType getCronType(CronStyle cronStyle) {
    switch (cronStyle) {
      case CRON4J:
        return CronType.CRON4J;
      case QUARTZ:
        return CronType.QUARTZ;
      case UNIX:
        return CronType.UNIX;
      case SPRING:
        return CronType.SPRING;
      case SPRING53:
        return CronType.SPRING53;
      default:
        throw new IllegalArgumentException(
            "No cron definition found for %s".formatted(this.cronStyle));
    }
  }

  @Override
  public boolean isDeterministic() {
    return true;
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CronSchedule)) return false;
    CronSchedule that = (CronSchedule) o;
    return Objects.equals(this.cronStyle, that.cronStyle)
        && Objects.equals(this.zoneId, that.zoneId)
        && Objects.equals(this.pattern, that.pattern);
  }

  @Override
  public final int hashCode() {
    return Objects.hash(cronStyle, zoneId, pattern);
  }

  @Override
  public String toString() {
    return "CronSchedule pattern=" + pattern + ", cronStyle=" + cronStyle + ", zone=" + zoneId;
  }

  public String getPattern() {
    return pattern;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  private static class DisabledScheduleExecutionTime implements ExecutionTime {
    @Override
    public Optional<ZonedDateTime> nextExecution(ZonedDateTime date) {
      throw unsupportedException();
    }

    @Override
    public Optional<Duration> timeToNextExecution(ZonedDateTime date) {
      throw unsupportedException();
    }

    @Override
    public Optional<ZonedDateTime> lastExecution(ZonedDateTime date) {
      throw unsupportedException();
    }

    @Override
    public Optional<Duration> timeFromLastExecution(ZonedDateTime date) {
      throw unsupportedException();
    }

    @Override
    public boolean isMatch(ZonedDateTime date) {
      throw unsupportedException();
    }

    private UnsupportedOperationException unsupportedException() {
      return new UnsupportedOperationException(
          "Schedule is marked as disabled. Method should never be called");
    }
  }

  @Override
  public boolean isDisabled() {
    return DISABLED.equals(pattern);
  }
}
