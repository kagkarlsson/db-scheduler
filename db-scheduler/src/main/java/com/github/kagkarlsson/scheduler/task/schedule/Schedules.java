/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task.schedule;

import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;

public class Schedules {
    private static final Parser SCHEDULE_PARSER = CompositeParser.of(new FixedDelayParser(), new DailyParser());

    public static Daily daily(LocalTime... times) {
        return new Daily(times);
    }

    public static Daily daily(ZoneId zone, LocalTime... times) {
        return new Daily(zone, times);
    }

    public static FixedDelay fixedDelay(Duration delay) {
        return FixedDelay.of(delay);
    }

    public static CronSchedule cron(String cronPattern) {
        return new CronSchedule(cronPattern);
    }

    public static CronSchedule cron(String cronPattern, ZoneId zoneId) {
        return new CronSchedule(cronPattern, zoneId);
    }

    /**
     * Currently supports Daily- and FixedDelay-schedule on the formats:
     * <pre>DAILY|hh:mm,hh:mm,...,hh:mm(|TIME_ZONE)</pre><br/>
     * <pre>FIXED_DELAY|xxxs  (xxx is number of seconds)</pre>
     *
     * @param scheduleString
     * @return A new schedule
     * @throws UnrecognizableSchedule When the scheduleString cannot be parsed
     */
    public static Schedule parseSchedule(String scheduleString) {
        return SCHEDULE_PARSER.parse(scheduleString)
            .orElseThrow(() -> new UnrecognizableSchedule(scheduleString, SCHEDULE_PARSER.examples()));
    }

    public static class UnrecognizableSchedule extends RuntimeException {
        public UnrecognizableSchedule(String inputSchedule) {
            super("Unrecognized schedule: '" + inputSchedule + "'");
        }

        public UnrecognizableSchedule(String inputSchedule, List<String> examples) {
            super("Unrecognized schedule: '" + inputSchedule + "'. Parsable examples: " + examples);
        }
    }
}
