/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task.schedule;

import com.github.kagkarlsson.scheduler.task.schedule.Schedules.UnrecognizableSchedule;

import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class DailyParser implements Parser {
    private static final Pattern DAILY_PATTERN_WITH_TIMEZONE = Pattern.compile("^DAILY\\|((\\d{2}:\\d{2})(,\\d{2}:\\d{2})*)(\\|(.+))?$");
    private static final List<String> EXAMPLES = Arrays.asList("DAILY|12:00", "DAILY|12:00,13:45", "DAILY|12:00,13:45|Europe/Rome");

    @Override
    public Schedule parse(String scheduleString) {
        MatchedSchedule matchedSchedule = match(scheduleString);
        return matchedSchedule.maybeZoneId()
            .map(zoneId -> new Daily(zoneId, matchedSchedule.scheduleTimes()))
            .orElse(new Daily(matchedSchedule.scheduleTimes()));
    }

    private MatchedSchedule match(String scheduleString) {
        return Optional.ofNullable(scheduleString)
            .map(DAILY_PATTERN_WITH_TIMEZONE::matcher)
            .filter(Matcher::matches)
            .map(MatchedSchedule::new)
            .orElseThrow(() -> new UnrecognizableSchedule(scheduleString, this.examples()));
    }

    @Override
    public List<String> examples() {
        return EXAMPLES;
    }

    private static class MatchedSchedule {
        private final Matcher matcher;

        private MatchedSchedule(Matcher matcher) {
            this.matcher = Objects.requireNonNull(matcher);
        }

        Optional<ZoneId> maybeZoneId() {
            return Optional.ofNullable(matcher.group(5)).map(toZoneId());
        }

        List<LocalTime> scheduleTimes() {
            String[] times = matcher.group(1).split(",");
            return Stream.of(times).map(timeStr -> {
                String[] hourAndMinute = timeStr.split(":");
                return LocalTime.of(Integer.parseInt(hourAndMinute[0]), Integer.parseInt(hourAndMinute[1]));
            }).collect(Collectors.toList());
        }

        private static Function<String, ZoneId> toZoneId() {
            return str -> {
                try {
                    return ZoneId.of(str);
                } catch (DateTimeException exception) {
                    throw new UnrecognizableSchedule(str, EXAMPLES);
                }
            };
        }
    }
}
